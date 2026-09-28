"""Health ejection/recovery and load-balancing behavior."""

import asyncio

import pytest

from gateway.config import GatewayConfig, RouteConfig, UpstreamConfig
from gateway.handler import GatewayHandler
from gateway.health import HealthState, HealthTracker, LoadBalancer
from tests.fakes import InstantSleep, ManualClock, ScriptedTransport


def make_config(**kw) -> GatewayConfig:
    return GatewayConfig(
        routes=(
            RouteConfig(
                prefix="/api",
                upstreams=(
                    UpstreamConfig(name="a1", base_url="http://a1"),
                    UpstreamConfig(name="a2", base_url="http://a2"),
                ),
                **kw,
            ),
        ),
        unhealthy_threshold=2,
        health_cooldown=10.0,
        failure_threshold=100,
        open_cooldown=100.0,
    )


def make_handler(config: GatewayConfig | None = None, transport=None, clock=None):
    config = config or make_config()
    tracker = HealthTracker(config.unhealthy_threshold, config.health_cooldown, clock=clock)
    lb = LoadBalancer(tracker)
    sleep = InstantSleep()
    handler = GatewayHandler(config, transport or ScriptedTransport(), load_balancer=lb, sleep=sleep)
    return handler, tracker, lb, sleep


@pytest.mark.asyncio
async def test_failed_instance_is_ejected_and_traffic_shifts() -> None:
    from gateway.transport import UpstreamRequest

    transport = ScriptedTransport()
    transport.set("http://a1", "unreachable")
    transport.set("http://a2", 200)
    handler, tracker, _, _ = make_handler(transport=transport)

    r1 = await handler.handle(UpstreamRequest("GET", "/api/x"))
    r2 = await handler.handle(UpstreamRequest("GET", "/api/x"))
    r3 = await handler.handle(UpstreamRequest("GET", "/api/x"))

    assert r1.response.status_code == 200
    assert r2.response.status_code == 200
    assert r3.response.status_code == 200
    # a1 got two failing calls then was ejected; a2 served all successes.
    assert len(transport.calls["http://a1"]) == 2
    assert tracker.state_of("/api", "a1") == HealthState.UNHEALTHY
    assert tracker.state_of("/api", "a2") == HealthState.HEALTHY
    # After ejection no further request goes to a1.
    assert all(req is not None for req in transport.calls["http://a2"])
    assert len(transport.calls["http://a2"]) >= 3


@pytest.mark.asyncio
async def test_ejected_instance_is_reprobed_after_cooldown_and_restored() -> None:
    from gateway.transport import UpstreamRequest

    clock = ManualClock()
    transport = ScriptedTransport()
    transport.set("http://a1", "unreachable", "unreachable", 200, 200)
    transport.set("http://a2", 200)
    handler, tracker, _, _ = make_handler(transport=transport, clock=clock)

    # Burn two failures on a1 to eject it.
    await handler.handle(UpstreamRequest("GET", "/api/x"))
    await handler.handle(UpstreamRequest("GET", "/api/x"))
    assert tracker.state_of("/api", "a1") == HealthState.UNHEALTHY

    # Before cooldown a1 is never probed: a2 serves.
    clock.advance(5)
    for _ in range(3):
        r = await handler.handle(UpstreamRequest("GET", "/api/x"))
        assert r.served_by == "a2"
    assert tracker.state_of("/api", "a1") == HealthState.UNHEALTHY

    # After cooldown one probe is admitted; it succeeds -> healthy again.
    clock.advance(6)
    r = await handler.handle(UpstreamRequest("GET", "/api/x"))
    assert r.response.status_code == 200
    assert tracker.state_of("/api", "a1") == HealthState.HEALTHY


@pytest.mark.asyncio
async def test_failed_probe_re_ejects_instance() -> None:
    from gateway.transport import UpstreamRequest

    clock = ManualClock()
    transport = ScriptedTransport()
    transport.set("http://a1", "unreachable")
    transport.set("http://a2", 200)
    handler, tracker, _, _ = make_handler(transport=transport, clock=clock)

    await handler.handle(UpstreamRequest("GET", "/api/x"))
    await handler.handle(UpstreamRequest("GET", "/api/x"))
    assert tracker.state_of("/api", "a1") == HealthState.UNHEALTHY
    ejected_at_before = tracker._ejected_at[("/api", "a1")]

    clock.advance(11)
    # a2 is healthy, so it is still preferred; force a probe only when a2 is down.
    transport.set("http://a2", "unreachable", "unreachable")
    # Two requests: a2 gets ejected during these, a1 may get a probe.
    await handler.handle(UpstreamRequest("GET", "/api/x"))
    await handler.handle(UpstreamRequest("GET", "/api/x"))

    # Both instances are now ejected: next request after a1 cooldown probes a1.
    clock.advance(11)
    r = await handler.handle(UpstreamRequest("GET", "/api/x"))
    # a1's probe fails: remains unhealthy, normalized failure surfaced.
    assert tracker.state_of("/api", "a1") == HealthState.UNHEALTHY
    assert tracker._ejected_at[("/api", "a1")] > ejected_at_before
    assert r.response is None
    assert r.reason in ("upstream_unreachable", "no_healthy_instance", "circuit_open")


@pytest.mark.asyncio
async def test_only_one_probe_per_ejected_instance_under_concurrency() -> None:
    clock = ManualClock()
    tracker = HealthTracker(2, 10.0, clock=clock)
    await tracker.record_failure("/api", "a1", reason="x")
    await tracker.record_failure("/api", "a1", reason="x")
    assert tracker.state_of("/api", "a1") == HealthState.UNHEALTHY
    clock.advance(20)

    results = await asyncio.gather(
        *[tracker.acquire_probe("/api", "a1") for _ in range(20)]
    )
    assert sum(1 for ok in results if ok) == 1
    assert tracker.state_of("/api", "a1") == HealthState.PROBING
