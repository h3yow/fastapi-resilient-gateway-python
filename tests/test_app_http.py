"""通过 FastAPI TestClient 验证边车的 HTTP 行为：转发、统一错误信封、诊断端点。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.errors import FailureReason, UpstreamUnavailable
from gateway.transport import UpstreamResponse
from tests.conftest import FakeTransport, ManualClock, build_config, make_gateway


def make_client(transport: FakeTransport, clock: ManualClock, cfg=None):
    cfg = cfg or build_config(circuit={"failure_threshold": 99})
    gw = make_gateway(cfg, transport, clock)
    app = create_app(gw, probe_interval=3600.0)  # 测试中不跑后台探测
    return TestClient(app), gw


def test_successful_proxy_and_diagnostic_endpoints(clock):
    async def ok(base, req):
        return UpstreamResponse(200, body=b'{"v":1}', headers={"content-type": ["application/json"]})

    client, _gw = make_client(FakeTransport({"http://a": ok, "http://b": ok}), clock)
    with client:
        r = client.get("/api/items/1")
        assert r.status_code == 200
        assert r.json() == {"v": 1}
        assert r.headers["x-gateway-upstream"] in {"a", "b"}

        assert client.get("/_gateway/healthz").json() == {"status": "ok"}
        state = client.get("/_gateway/state").json()
        assert set(state["health"]) == {"a", "b"}
        events = client.get("/_gateway/events").json()["events"]
        assert isinstance(events, list)


def test_error_envelope_is_normalized_and_distinguishable(clock):
    async def boom(base, req):
        raise UpstreamUnavailable(FailureReason.UPSTREAM_CONNECTION, "refused")

    cfg = build_config(
        retry={"max_attempts": 1, "base_delay": 0.0, "jitter": 0.0},
    )
    client, _ = make_client(FakeTransport({"http://a": boom, "http://b": boom}), clock, cfg)
    with client:
        r = client.get("/api/x")
        assert r.status_code in {502, 503}
        body = r.json()
        assert body["error"]["reason"] in {
            FailureReason.UPSTREAM_CONNECTION.value,
            FailureReason.NO_HEALTHY_UPSTREAM.value,
        }
        assert r.headers["x-gateway-error"] == body["error"]["reason"]


def test_circuit_open_envelope(clock):
    async def boom(base, req):
        raise UpstreamUnavailable(FailureReason.UPSTREAM_CONNECTION, "refused")

    from gateway.config import load_config

    cfg = load_config(
        {
            "upstreams": [
                {"id": "a", "base_url": "http://a", "failure_threshold": 99,
                 "recover_successes": 1, "probe_interval": 5},
            ],
            "routes": [
                {
                    "id": "r",
                    "prefix": "/api",
                    "upstream_ids": ["a"],
                    "retry": {"max_attempts": 1},
                    "circuit": {"failure_threshold": 1, "open_cooldown": 60.0},
                }
            ],
        }
    )
    client, _ = make_client(FakeTransport({"http://a": boom}), clock, cfg)
    with client:
        r1 = client.get("/api/x")
        assert r1.json()["error"]["reason"] == "upstream_connection"
        # 第一次即熔断（阈值 1），后续立即降级
        r2 = client.get("/api/x")
        assert r2.status_code == 503
        assert r2.json()["error"]["reason"] == "circuit_open"


def test_idempotency_conflict_http_status(clock):
    import asyncio
    import threading
    import time

    async def slow(base, req):
        await asyncio.sleep(0.3)
        return UpstreamResponse(201)

    client, _ = make_client(FakeTransport({"http://a": slow, "http://b": slow}), clock)
    with client:
        results = []

        def hit():
            results.append(
                client.post("/api/x", headers={"idempotency-key": "k"}, json={})
            )

        # 后台线程持键执行期间，主线程同键请求应得到 409
        t = threading.Thread(target=hit)
        t.start()
        time.sleep(0.1)
        r = client.post("/api/x", headers={"idempotency-key": "k"}, json={})
        t.join()

        assert r.status_code == 409
        assert r.json()["error"]["reason"] == "idempotency_conflict"
        assert results[0].status_code == 201
