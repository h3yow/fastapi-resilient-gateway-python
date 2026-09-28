"""网关级：熔断降级结果可区分、跨实例切换、重试上限、4xx 不重试、并发稳定。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.errors import (
    FailureReason,
    GatewayError,
    UpstreamUnavailable,
)
from gateway.transport import UpstreamRequest, UpstreamResponse
from tests.conftest import FakeTransport, build_config, make_gateway, ok_response


def fail_conn():
    async def fn(base_url: str, request: UpstreamRequest) -> UpstreamResponse:
        raise UpstreamUnavailable(FailureReason.UPSTREAM_CONNECTION, "refused")

    return fn


def status_of(code: int):
    async def fn(base_url: str, request: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(code, body=f"{code}".encode())

    return fn


@pytest.mark.asyncio
async def test_open_circuit_returns_distinguishable_degraded_response(clock, event_log):
    """单实例路由熔断后：返回 503 + circuit_open，且不再打到上游。"""
    # 改成单上游路由
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
                    "circuit": {"failure_threshold": 2, "open_cooldown": 10.0},
                }
            ],
        }
    )
    transport = FakeTransport({"http://a": fail_conn()})
    gw = make_gateway(cfg, transport, clock, event_log)

    errors = []
    for _ in range(2):
        try:
            await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
        except GatewayError as e:
            errors.append(e)
    # 前两次都是真实尝试（连接失败归一化）
    assert errors[0].reason is FailureReason.UPSTREAM_CONNECTION
    upstream_calls = len(transport.calls)
    assert upstream_calls >= 2

    # 熔断已开：后续请求直接降级，不再有上游调用
    for _ in range(5):
        with pytest.raises(GatewayError) as ei:
            await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
        assert ei.value.reason is FailureReason.CIRCUIT_OPEN
        assert ei.value.status_code == 503
    assert len(transport.calls) == upstream_calls


@pytest.mark.asyncio
async def test_circuit_open_fails_over_to_other_instance(clock, event_log):
    """一个实例熔断后，请求切到另一个实例，正常返回。"""
    cfg = build_config(
        retry={"max_attempts": 1, "base_delay": 0.0, "jitter": 0.0},
        circuit={"failure_threshold": 1, "open_cooldown": 10.0},
    )
    transport = FakeTransport({"http://a": fail_conn(), "http://b": ok_response()})
    gw = make_gateway(cfg, transport, clock, event_log)

    # 第一次请求：a 被选中失败且熔断（attempts 只有 1，直接抛连接错误）
    with pytest.raises(GatewayError) as ei:
        await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
    assert ei.value.reason is FailureReason.UPSTREAM_CONNECTION

    # 之后：a 健康摘除 + 熔断，流量全部去 b
    for _ in range(6):
        resp = await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
        assert resp.status_code == 200
        assert resp.headers["x-gateway-upstream"] == ["b"]
    assert all(c[0] != "http://a" for c in transport.calls[len(transport.calls) - 6:])


@pytest.mark.asyncio
async def test_retry_has_cap_and_normalized_last_reason(clock, event_log):
    cfg = build_config(
        retry={"max_attempts": 3, "base_delay": 0.0, "jitter": 0.0},
        circuit={"failure_threshold": 99, "open_cooldown": 100.0},
    )
    transport = FakeTransport({"http://a": status_of(500), "http://b": status_of(503)})
    gw = make_gateway(cfg, transport, clock, event_log)

    with pytest.raises(GatewayError) as ei:
        await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
    err = ei.value
    assert err.reason is FailureReason.RETRIES_EXHAUSTED
    assert err.details["attempts"] == 3
    assert err.details["last_reason"] in {"upstream_server"}
    assert len(transport.calls) == 3


@pytest.mark.asyncio
async def test_non_retryable_4xx_short_circuits_without_retries(clock, event_log):
    cfg = build_config(
        retry={"max_attempts": 5, "base_delay": 0.0, "jitter": 0.0},
        circuit={"failure_threshold": 99},
    )
    transport = FakeTransport({"http://a": status_of(404), "http://b": ok_response()})
    gw = make_gateway(cfg, transport, clock, event_log)

    with pytest.raises(GatewayError) as ei:
        await gw.forward(method="GET", path="/api/missing", headers={}, body=b"")
    assert ei.value.reason is FailureReason.UPSTREAM_HTTP
    assert ei.value.status_code == 404
    assert len(transport.calls) == 1  # 确定性失败，立即返回不重试


@pytest.mark.asyncio
async def test_429_is_retryable_and_recovers_on_other_instance(clock, event_log):
    cfg = build_config(
        retry={"max_attempts": 2, "base_delay": 0.0, "jitter": 0.0},
        circuit={"failure_threshold": 99},
    )
    transport = FakeTransport({"http://a": status_of(429), "http://b": ok_response()})
    gw = make_gateway(cfg, transport, clock, event_log)
    resp = await gw.forward(method="GET", path="/api/x", headers={}, body=b"")
    assert resp.status_code == 200
    assert len(transport.calls) == 2  # a 429 -> 重试 b 成功


@pytest.mark.asyncio
async def test_post_not_retried_by_default_but_idempotent_post_is(clock, event_log):
    cfg_post = build_config(
        retry={"max_attempts": 3, "base_delay": 0.0, "jitter": 0.0},
        circuit={"failure_threshold": 99},
    )
    transport = FakeTransport({"http://a": fail_conn(), "http://b": ok_response()})
    gw = make_gateway(cfg_post, transport, clock, event_log)

    # 无幂等键 POST：只尝试一次，不切换实例重试
    with pytest.raises(GatewayError) as ei:
        await gw.forward(method="POST", path="/api/orders", headers={}, body=b'{"x":1}')
    assert ei.value.reason is FailureReason.UPSTREAM_CONNECTION
    assert len(transport.calls) == 1

    # 带幂等键 POST（独立网关，保证首个选中的是 a）：允许安全重试，切到 b 成功
    gw2 = make_gateway(cfg_post, transport, clock, event_log)
    resp = await gw2.forward(
        method="POST",
        path="/api/orders",
        headers={"idempotency-key": "key-1"},
        body=b'{"x":1}',
    )
    assert resp.status_code == 200
    keyed_calls = [c for c in transport.calls if c[3] == "key-1"]
    assert len(keyed_calls) == 2
    assert {c[0] for c in keyed_calls} == {"http://a", "http://b"}


@pytest.mark.asyncio
async def test_concurrent_traffic_against_single_failing_instance_is_stable(clock, event_log):
    """并发请求同时打向会失败的实例：熔断只迁移一次，降级结论一致。"""
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
                    "circuit": {"failure_threshold": 3, "open_cooldown": 100.0},
                }
            ],
        }
    )
    transport = FakeTransport({"http://a": fail_conn()})
    gw = make_gateway(cfg, transport, clock, event_log)

    outcomes = await asyncio.gather(
        *[
            gw.forward(method="GET", path="/api/x", headers={}, body=b"")
            for _ in range(30)
        ],
        return_exceptions=True,
    )
    reasons = {o.reason for o in outcomes if isinstance(o, GatewayError)}
    assert reasons <= {FailureReason.UPSTREAM_CONNECTION, FailureReason.CIRCUIT_OPEN}
    assert FailureReason.CIRCUIT_OPEN in reasons
    opens = [
        e for e in event_log.of_kind("circuit_state") if "closed -> open" in e.message
    ]
    assert len(opens) == 1
