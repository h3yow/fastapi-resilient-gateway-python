"""Circuit breaker open/half-open/closed transitions incl. concurrency."""

import asyncio

import pytest

from gateway.breaker import BreakerState, CircuitBreaker
from gateway.config import GatewayConfig, RouteConfig, UpstreamConfig
from gateway.handler import GatewayHandler
from gateway.health import HealthTracker, LoadBalancer
from tests.fakes import InstantSleep, ManualClock, ScriptedTransport


def make_config(
    failure_threshold: int = 3,
    open_cooldown: float = 10.0,
    unhealthy_threshold: int = 100,
    upstreams=(
        UpstreamConfig(name="a1", base_url="http://a1"),
        UpstreamConfig(name="a2", base_url="http://a2"),
    ),
    **route_kw,
) -> GatewayConfig:
    return GatewayConfig(
        routes=(
            RouteConfig(
                prefix="/api",
                upstreams=tuple(upstreams),
                **route_kw,
            ),
        ),
        unhealthy_threshold=unhealthy_threshold,
        health_cooldown=100.0,
        failure_threshold=failure_threshold,
        open_cooldown=open_cooldown,
    )


def make_handler(config, transport, clock):
    tracker = HealthTracker(config.unhealthy_threshold, config.health_cooldown, clock=clock)
    lb = LoadBalancer(tracker)
    breaker = CircuitBreaker(config.failure_threshold, config.open_cooldown, clock=clock)
    events = []
    breaker._on_event = lambda e: events.append(e)
    handler = GatewayHandler(
        config, transport, load_balancer=lb, breaker=breaker, sleep=InstantSleep()
    )
    lb.reset_counters()
    return handler, breaker, events


@pytest.mark.asyncio
async def test_breaker_opens_after_threshold_and_fast_fails() -> None:
    from gateway.transport import UpstreamRequest

    clock = ManualClock()
    transport = ScriptedTransport()
    transport.set("http://a1", "unreachable")
    transport.set("http://a2", "unreachable")
    single = (UpstreamConfig(name="a1", base_url="http://a1"),)
    config = make_config(failure_threshold=3, unhealthy_threshold=100, upstreams=single)
    handler, breaker, events = make_handler(config, transport, clock)

    # 3 attempts all hit the single instance a1 and open its circuit.
    r = await handler.handle(UpstreamRequest("GET", "/api/x"))
    assert breaker.state_of("/api", "a1") == BreakerState.OPEN
    assert r.reason == "upstream_unreachable"
    transitions = [(e.old_state, e.new_state) for e in events]
    assert (BreakerState.CLOSED, BreakerState.OPEN) in transitions

    # While open, a1 is never called again: the request fast-fails degraded.
    calls_a1_before = len(transport.calls["http://a1"])
    r2 = await handler.handle(UpstreamRequest("GET", "/api/x"))
    assert len(transport.calls["http://a1"]) == calls_a1_before
    assert r2.reason == "circuit_open"
    assert r2.response is None


@pytest.mark.asyncio
async def test_half_open_probe_success_closes_circuit() -> None:
    from gateway.transport import UpstreamRequest

    clock = ManualClock()
    transport = ScriptedTransport()
    transport.set("http://a1", "unreachable", "unreachable", "unreachable", 200)
    single = (UpstreamConfig(name="a1", base_url="http://a1"),)
    config = make_config(failure_threshold=3, unhealthy_threshold=100, upstreams=single)
    handler, breaker, events = make_handler(config, transport, clock)

    await handler.handle(UpstreamRequest("GET", "/api/x"))
    assert breaker.state_of("/api", "a1") == BreakerState.OPEN

    clock.advance(11)
    # Cooldown elapsed: exactly one probe is admitted, and it succeeds.
    r = await handler.handle(UpstreamRequest("GET", "/api/x"))
    assert breaker.state_of("/api", "a1") == BreakerState.CLOSED
    assert (BreakerState.HALF_OPEN, BreakerState.CLOSED) in [
        (e.old_state, e.new_state) for e in events
    ]
    assert r.response is not None and r.response.status_code == 200


@pytest.mark.asyncio
async def test_half_open_probe_failure_reopens() -> None:
    clock = ManualClock()
    breaker = CircuitBreaker(3, 10.0, clock=clock)
    for _ in range(3):
        await breaker.record_failure("/api", "a1", reason="x")
    assert breaker.state_of("/api", "a1") == BreakerState.OPEN
    opened_at = breaker._opened_at[("/api", "a1")]

    clock.advance(11)
    d = await breaker.allow("/api", "a1")
    assert d.allowed and d.is_probe
    assert breaker.state_of("/api", "a1") == BreakerState.HALF_OPEN

    await breaker.record_failure("/api", "a1", reason="probe")
    assert breaker.state_of("/api", "a1") == BreakerState.OPEN
    assert breaker._opened_at[("/api", "a1")] > opened_at

    # Fresh cooldown applies again.
    d2 = await breaker.allow("/api", "a1")
    assert not d2.allowed and d2.state == BreakerState.OPEN


@pytest.mark.asyncio
async def test_concurrent_requests_dont_flap_half_open_state() -> None:
    clock = ManualClock()
    breaker = CircuitBreaker(3, 10.0, clock=clock)
    for _ in range(3):
        await breaker.record_failure("/api", "a1", reason="x")
    clock.advance(11)

    decisions = await asyncio.gather(*[breaker.allow("/api", "a1") for _ in range(50)])
    probes = [d for d in decisions if d.is_probe]
    allowed = [d for d in decisions if d.allowed]
    assert len(probes) == 1
    assert len(allowed) == 1  # only the single probe is admitted
    assert breaker.state_of("/api", "a1") == BreakerState.HALF_OPEN

    # While probing, more concurrent traffic is fast-failed.
    more = await asyncio.gather(*[breaker.allow("/api", "a1") for _ in range(50)])
    assert all(not d.allowed for d in more)


@pytest.mark.asyncio
async def test_concurrent_failures_open_exactly_once() -> None:
    breaker = CircuitBreaker(10, 10.0)
    events = []
    breaker._on_event = lambda e: events.append(e)

    async def fail_one() -> None:
        await breaker.record_failure("/api", "a1", reason="x")

    await asyncio.gather(*[fail_one() for _ in range(30)])
    opens = [e for e in events if e.new_state == BreakerState.OPEN]
    assert len(opens) == 1
    assert breaker.state_of("/api", "a1") == BreakerState.OPEN
