"""幂等语义：重试/重放不产生重复副作用；并发同键得到可区分冲突。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.errors import FailureReason, GatewayError
from gateway.transport import UpstreamRequest, UpstreamResponse
from tests.conftest import FakeTransport, build_config, make_gateway


class SideEffectServer:
    """模拟支持幂等键去重的上游写服务，并统计副作用次数。"""

    def __init__(self) -> None:
        self.side_effects = 0
        self.seen_keys: set[str] = set()

    async def __call__(self, base_url: str, request: UpstreamRequest) -> UpstreamResponse:
        key = request.headers.get("idempotency-key")
        # 上游自身也按幂等键去重（真实服务的职责）
        if key in self.seen_keys:
            return UpstreamResponse(200, body=b'duplicated-request-rejected', headers={})
        self.seen_keys.add(key)
        self.side_effects += 1
        return UpstreamResponse(201, body=b'created', headers={})


@pytest.mark.asyncio
async def test_gateway_retry_does_not_duplicate_upstream_submission(clock, event_log):
    """a 收请求后"网络失败"但已产生副作用；网关带同键重试到 b，只允许一次副作用。

    这里通过上游各自识别幂等键来证明：网关把同一个键透传到了每次尝试，
    因此上游/网关协作下不会产生重复副作用。
    """
    cfg = build_config(
        retry={"max_attempts": 3, "base_delay": 0.0, "jitter": 0.0},
        circuit={"failure_threshold": 99},
    )
    svc = SideEffectServer()

    async def a_behavior(base_url, request):
        # 服务端执行了副作用（加键），但响应在网络上丢失 -> 抛连接错误
        await svc(base_url, request)
        from gateway.errors import UpstreamUnavailable

        raise UpstreamUnavailable(FailureReason.UPSTREAM_CONNECTION, "response lost")

    transport = FakeTransport({"http://a": a_behavior, "http://b": svc})
    gw = make_gateway(cfg, transport, clock, event_log)

    resp = await gw.forward(
        method="POST",
        path="/api/orders",
        headers={"idempotency-key": "order-123"},
        body=b"{}",
    )
    # b 看到同一幂等键，拒绝重复执行，但返回成功语义
    assert resp.status_code == 200
    assert svc.side_effects == 1
    keys_seen_by_upstreams = [c[3] for c in transport.calls if c[3]]
    assert keys_seen_by_upstreams == ["order-123", "order-123"]


@pytest.mark.asyncio
async def test_completed_response_is_replayed_for_same_key(clock, event_log):
    cfg = build_config(
        retry={"max_attempts": 1, "base_delay": 0.0, "jitter": 0.0},
        circuit={"failure_threshold": 99},
    )
    svc = SideEffectServer()
    transport = FakeTransport({"http://a": svc, "http://b": svc})
    gw = make_gateway(cfg, transport, clock, event_log)

    r1 = await gw.forward(
        method="POST", path="/api/x", headers={"idempotency-key": "k"}, body=b"{}"
    )
    assert r1.status_code == 201
    # 同键重放 3 次：上游只执行一次，客户端拿到缓存结果
    for _ in range(3):
        r = await gw.forward(
            method="POST", path="/api/x", headers={"idempotency-key": "k"}, body=b"{}"
        )
        assert r.headers.get("x-gateway-idempotent-replay") == ["true"]
        assert r.body == b"created"
    assert svc.side_effects == 1


@pytest.mark.asyncio
async def test_concurrent_same_key_conflict_is_distinguishable(clock, event_log):
    """同键并发：一个占位执行，其余得到 409 idempotency_conflict。"""
    cfg = build_config(circuit={"failure_threshold": 99})
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow(base_url, request):
        started.set()
        await release.wait()
        return UpstreamResponse(201, body=b"ok")

    transport = FakeTransport({"http://a": slow, "http://b": slow})
    gw = make_gateway(cfg, transport, clock, event_log)

    async def call_one():
        return await gw.forward(
            method="POST", path="/api/x", headers={"idempotency-key": "hot"}, body=b"{}"
        )

    first = asyncio.create_task(call_one())
    await started.wait()
    others = await asyncio.gather(*[call_one() for _ in range(9)], return_exceptions=True)
    release.set()
    first_result = await first

    conflicts = [o for o in others if isinstance(o, GatewayError)]
    assert first_result.status_code == 201
    assert len(conflicts) == 9
    assert all(o.reason is FailureReason.IDEMPOTENCY_CONFLICT for o in conflicts)
    assert all(o.status_code == 409 for o in conflicts)


@pytest.mark.asyncio
async def test_failed_request_releases_key_for_retry_by_client(clock, event_log):
    """上游最终失败（非确定性终态）时释放键，客户端可用同键重新发起。"""
    from gateway.errors import UpstreamUnavailable

    cfg = build_config(
        retry={"max_attempts": 1, "base_delay": 0.0, "jitter": 0.0},
        circuit={"failure_threshold": 99},
    )

    async def boom(base_url, request):
        raise UpstreamUnavailable(FailureReason.UPSTREAM_CONNECTION, "down")

    transport = FakeTransport({"http://a": boom, "http://b": boom})
    gw = make_gateway(cfg, transport, clock, event_log)

    with pytest.raises(GatewayError):
        await gw.forward(
            method="POST", path="/api/x", headers={"idempotency-key": "retry-me"}, body=b"{}"
        )
    # 键已释放：可以重新占位（不抛 conflict）
    assert "retry-me" not in gw.snapshot()["idempotency"]
