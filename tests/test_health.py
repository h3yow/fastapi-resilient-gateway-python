"""健康摘除与恢复：切换期间流量绝不打到已判定不健康的实例，恢复后自动纳入。"""

from __future__ import annotations

import pytest

from gateway.errors import FailureReason, UpstreamUnavailable
from gateway.transport import UpstreamRequest, UpstreamResponse
from tests.conftest import FakeTransport, build_config, make_gateway, ok_response


def conn_fail(base_url: str, request: UpstreamRequest) -> UpstreamResponse:
    raise UpstreamUnavailable(FailureReason.UPSTREAM_CONNECTION, "boom")


@pytest.mark.asyncio
async def test_traffic_never_hits_unhealthy_instance_and_fails_over(clock, event_log):
    cfg = build_config()  # failure_threshold=3
    transport = FakeTransport({"http://a": conn_fail, "http://b": ok_response()})
    gw = make_gateway(cfg, transport, clock, event_log)

    # 前两轮：a 拿到请求并失败，流量被重试切到 b
    for _ in range(3):
        resp = await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
        assert resp.status_code == 200
        assert resp.headers["x-gateway-upstream"] == ["b"]

    # a 此时应已被摘除
    snap = gw.snapshot()
    assert snap["health"]["a"]["state"] == "unhealthy"
    assert snap["health"]["b"]["state"] == "healthy"

    # 再多打若干请求：绝不允许打到 a
    a_hits = sum(1 for c in transport.calls if c[0] == "http://a")
    before = len(transport.calls)
    for _ in range(8):
        resp = await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
        assert resp.headers["x-gateway-upstream"] == ["b"]
    after = len(transport.calls)
    assert after - before == 8
    assert sum(1 for c in transport.calls[before:] if c[0] == "http://a") == 0
    assert a_hits <= 3  # 摘除后不再新增

    changes = [(e.target, e.message) for e in event_log.of_kind("health_changed")]
    assert any(t == "a" and "-> unhealthy" in m for t, m in changes)


@pytest.mark.asyncio
async def test_probe_recovers_instance_and_it_receives_traffic(clock, event_log):
    cfg = build_config()
    transport = FakeTransport({"http://a": conn_fail, "http://b": ok_response()})
    gw = make_gateway(cfg, transport, clock, event_log)

    # 打挂 a
    for _ in range(3):
        await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
    assert gw.snapshot()["health"]["a"]["state"] == "unhealthy"

    # 探测前两次失败（recover_successes=2），仍摘除
    transport.healthy["http://a"] = False
    assert await gw._health.probe("a", transport) is False
    assert gw._health.probe_due("a") is False  # 距上次探测不足 interval
    clock.advance(0.6)
    assert await gw._health.probe("a", transport) is False
    assert gw.snapshot()["health"]["a"]["state"] == "unhealthy"

    # a 恢复：连续两次探测成功才重新纳入；真实业务行为也恢复
    transport.healthy["http://a"] = True
    transport.behavior["http://a"] = ok_response()
    clock.advance(0.6)
    await gw.run_probes()
    assert gw.snapshot()["health"]["a"]["state"] == "unhealthy"
    clock.advance(0.6)
    await gw.run_probes()
    assert gw.snapshot()["health"]["a"]["state"] == "healthy"

    # 恢复后流量重新在 a,b 间轮询（b 先发：计数器停在摘除后的下一位置）
    targets = []
    for _ in range(4):
        resp = await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
        targets.append(resp.headers["x-gateway-upstream"][0])
    assert set(targets) == {"a", "b"}

    rec = [(e.target, e.message) for e in event_log.of_kind("health_changed")]
    assert any("unhealthy -> healthy (probe recovery)" in m for t, m in rec if t == "a")


@pytest.mark.asyncio
async def test_all_instances_down_returns_normalized_no_healthy(clock, event_log):
    cfg = build_config(retry={"max_attempts": 2, "base_delay": 0.0, "jitter": 0.0})
    transport = FakeTransport({"http://a": conn_fail, "http://b": conn_fail})
    gw = make_gateway(cfg, transport, clock, event_log)

    from gateway.errors import GatewayError

    # 第一轮尝试后两者都至少 SUSPECT/UNHEALTHY；持续请求最终得到 no_healthy_upstream
    reason_seen = set()
    for _ in range(6):
        try:
            await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
        except GatewayError as e:
            reason_seen.add(e.reason)
    assert FailureReason.NO_HEALTHY_UPSTREAM in reason_seen
    assert gw.snapshot()["health"]["a"]["state"] == "unhealthy"
    assert gw.snapshot()["health"]["b"]["state"] == "unhealthy"
