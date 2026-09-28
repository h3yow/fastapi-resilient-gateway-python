"""Retry safety/budget/backoff and idempotent duplicate suppression."""

import asyncio

import pytest

from gateway.config import GatewayConfig, RouteConfig, UpstreamConfig
from gateway.handler import GatewayHandler
from gateway.retry import RetryPolicy
from gateway.health import HealthTracker, LoadBalancer
from gateway.breaker import CircuitBreaker
from gateway.transport import UpstreamRequest
from tests.fakes import InstantSleep, ScriptedTransport


def make_config(methods_policy: RetryPolicy | None = None) -> GatewayConfig:
    return GatewayConfig(
        routes=(
            RouteConfig(
                prefix="/api",
                upstreams=(
                    UpstreamConfig(name="a1", base_url="http://a1"),
                    UpstreamConfig(name="a2", base_url="http://a2"),
                ),
                retry_policy=methods_policy or RetryPolicy(max_attempts=3),
            ),
        ),
        unhealthy_threshold=100,
        health_cooldown=100.0,
        failure_threshold=100,
        open_cooldown=100.0,
    )


def make_handler(config, transport):
    tracker = HealthTracker(config.unhealthy_threshold, config.health_cooldown)
    lb = LoadBalancer(tracker)
    breaker = CircuitBreaker(config.failure_threshold, config.open_cooldown)
    sleep = InstantSleep()
    handler = GatewayHandler(
        config, transport, load_balancer=lb, breaker=breaker,
        sleep=sleep, random_func=lambda: 0.5,
    )
    lb.reset_counters()
    return handler, sleep


@pytest.mark.asyncio
async def test_get_is_retried_up_to_max_attempts_with_backoff() -> None:
    transport = ScriptedTransport()
    transport.set("http://a1", "timeout")
    transport.set("http://a2", "unreachable")
    handler, sleep = make_handler(make_config(), transport)

    r = await handler.handle(UpstreamRequest("GET", "/api/x"))
    assert r.response is None
    assert r.attempts == 3
    assert r.reason in ("upstream_timeout", "upstream_unreachable")
    # two backoff sleeps for 3 attempts, full jitter factor 0.5
    assert len(sleep.delays) == 2
    assert sleep.delays[0] == pytest.approx(0.5 * 0.05)
    assert sleep.delays[1] == pytest.approx(0.5 * 0.10)
    # attempts spread across both instances
    total = len(transport.calls["http://a1"]) + len(transport.calls["http://a2"])
    assert total == 3


@pytest.mark.asyncio
async def test_unsafe_post_without_idempotency_key_is_not_retried() -> None:
    transport = ScriptedTransport()
    transport.set("http://a1", "unreachable")
    transport.set("http://a2", "unreachable")
    handler, sleep = make_handler(make_config(), transport)

    r = await handler.handle(UpstreamRequest("POST", "/api/orders", body=b"{}"))
    assert r.attempts == 1
    assert r.reason == "upstream_unreachable"
    assert sleep.delays == []
    total = len(transport.calls["http://a1"]) + len(transport.calls["http://a2"])
    assert total == 1


@pytest.mark.asyncio
async def test_post_with_idempotency_key_is_retried() -> None:
    transport = ScriptedTransport()
    transport.set("http://a1", "unreachable")
    transport.set("http://a2", "unreachable")
    handler, _ = make_handler(make_config(), transport)

    req = UpstreamRequest(
        "POST", "/api/orders", headers={"Idempotency-Key": "k-1"}, body=b"{}"
    )
    r = await handler.handle(req)
    assert r.attempts == 3
    total = len(transport.calls["http://a1"]) + len(transport.calls["http://a2"])
    assert total == 3
    # key forwarded upstream
    for calls in transport.calls.values():
        for c in calls:
            assert c.headers.get("idempotency-key") == "k-1"


@pytest.mark.asyncio
async def test_4xx_is_passed_through_and_not_retried() -> None:
    transport = ScriptedTransport()
    transport.set("http://a1", 400)
    handler, sleep = make_handler(make_config(), transport)

    r = await handler.handle(UpstreamRequest("GET", "/api/bad"))
    assert r.response is not None
    assert r.response.status_code == 400
    assert r.reason is None
    assert len(transport.calls["http://a1"]) == 1
    assert sleep.delays == []


@pytest.mark.asyncio
async def test_5xx_retries_then_returns_annotated_upstream_response() -> None:
    transport = ScriptedTransport()
    transport.set("http://a1", 500)
    transport.set("http://a2", 503)
    handler, _ = make_handler(make_config(RetryPolicy(max_attempts=2)), transport)

    r = await handler.handle(UpstreamRequest("GET", "/api/boom"))
    assert r.response is not None
    assert r.response.status_code in (500, 503)
    assert r.reason in ("upstream_5xx_500", "upstream_5xx_503")
    assert r.response.headers["x-gateway-failure"] == r.reason


@pytest.mark.asyncio
async def test_idempotency_replay_does_not_reexecute_upstream() -> None:
    transport = ScriptedTransport()
    transport.set("http://a1", 200)
    handler, _ = make_handler(make_config(), transport)

    req = UpstreamRequest(
        "POST", "/api/orders", headers={"Idempotency-Key": "dedup-1"}, body=b"{}"
    )
    r1 = await handler.handle(req)
    r2 = await handler.handle(req)
    assert r1.response.status_code == 200
    assert r2.response.status_code == 200
    total = len(transport.calls["http://a1"]) + len(transport.calls["http://a2"])
    assert total == 1  # second replay served from store
    assert r2.attempts == 0


@pytest.mark.asyncio
async def test_concurrent_idempotent_requests_execute_once() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def scripted_send(base_url, request, *, timeout):
        started.set()
        await release.wait()
        from gateway.transport import UpstreamResponse
        return UpstreamResponse(status_code=201, body=b"created")

    class SlowTransport:
        calls = 0

        async def send(self, base_url, request, *, timeout):
            SlowTransport.calls += 1
            return await scripted_send(base_url, request, timeout=timeout)

    transport = SlowTransport()
    handler, _ = make_handler(make_config(), transport)
    req = UpstreamRequest(
        "POST", "/api/orders", headers={"Idempotency-Key": "concurrent-1"}
    )

    async def call():
        return await handler.handle(req)

    task = asyncio.create_task(call())
    # Wait until the owner is parked inside the upstream call, then launch
    # concurrent replays which must coalesce on the same in-flight future.
    await started.wait()
    await asyncio.sleep(0)
    others_task = asyncio.gather(*[call() for _ in range(9)])
    await asyncio.sleep(0)
    release.set()
    first = await task
    others = await others_task

    assert SlowTransport.calls == 1
    assert first.response.status_code == 201
    assert all(r.response.status_code == 201 for r in others)


@pytest.mark.asyncio
async def test_failed_idempotent_request_releases_key_for_retry() -> None:
    from dataclasses import replace

    base = make_config(RetryPolicy(max_attempts=1))
    single = (UpstreamConfig(name="a1", base_url="http://a1"),)
    config = GatewayConfig(
        routes=(replace(base.routes[0], upstreams=single),),
        unhealthy_threshold=base.unhealthy_threshold,
        health_cooldown=base.health_cooldown,
        failure_threshold=base.failure_threshold,
        open_cooldown=base.open_cooldown,
    )
    transport = ScriptedTransport()
    transport.set("http://a1", "unreachable", 200)
    handler, _ = make_handler(config, transport)

    req = UpstreamRequest("POST", "/api/x", headers={"Idempotency-Key": "k-fail"})
    r1 = await handler.handle(req)
    assert r1.reason == "upstream_unreachable"

    # Key released: a later identical request is allowed to execute again.
    r2 = await handler.handle(req)
    assert r2.response is not None
    assert r2.response.status_code == 200
