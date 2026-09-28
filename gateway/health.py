"""Instance health tracking and load balancing with ejection/recovery."""

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Callable

from gateway.config import UpstreamConfig

logger = logging.getLogger("gateway.health")


class HealthState(str, Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"  # ejected, cooling down
    PROBING = "probing"      # one recovery probe in flight


@dataclass(frozen=True)
class HealthEvent:
    route: str
    upstream: str
    old_state: HealthState
    new_state: HealthState
    reason: str
    consecutive_failures: int


EventHandler = Callable[[HealthEvent], None]


@dataclass(frozen=True)
class Selection:
    upstream: UpstreamConfig
    is_probe: bool


def _monotonic() -> float:
    return time.monotonic()


class HealthTracker:
    """Tracks per-(route, upstream) health with async locks.

    Failure policy: ``unhealthy_threshold`` consecutive failures eject an
    instance; after ``cooldown`` seconds one probe is admitted; failure
    re-ejects (with a fresh cooldown), success restores. All state changes
    happen under a per-instance lock, so concurrent requests cannot double
    eject or admit multiple recovery probes.
    """

    def __init__(
        self,
        unhealthy_threshold: int,
        cooldown: float,
        *,
        clock: Callable[[], float] | None = None,
        on_event: EventHandler | None = None,
    ) -> None:
        self._unhealthy_threshold = unhealthy_threshold
        self._cooldown = cooldown
        self._clock = clock or _monotonic
        self._on_event = on_event
        self._states: dict[tuple[str, str], HealthState] = {}
        self._failures: dict[tuple[str, str], int] = defaultdict(int)
        self._ejected_at: dict[tuple[str, str], float] = {}
        self._locks: dict[tuple[str, str], asyncio.Lock] = defaultdict(asyncio.Lock)

    def _key(self, route: str, upstream: str) -> tuple[str, str]:
        return (route, upstream)

    def state_of(self, route: str, upstream: str) -> HealthState:
        return self._states.get(self._key(route, upstream), HealthState.HEALTHY)

    def is_available(self, route: str, upstream: str, *, now: float | None = None) -> bool:
        """Healthy instances serve traffic; ejected ones admit one probe after cooldown."""
        key = self._key(route, upstream)
        state = self._states.get(key, HealthState.HEALTHY)
        if state in (HealthState.HEALTHY,):
            return True
        if state == HealthState.PROBING:
            return False
        if now is None:
            now = self._clock()
        return now - self._ejected_at.get(key, 0.0) >= self._cooldown

    def _emit(
        self,
        key: tuple[str, str],
        old: HealthState,
        new: HealthState,
        reason: str,
    ) -> None:
        event = HealthEvent(
            route=key[0],
            upstream=key[1],
            old_state=old,
            new_state=new,
            reason=reason,
            consecutive_failures=self._failures[key],
        )
        logger.info(
            "health %s/%s: %s -> %s (%s, failures=%d)",
            event.route,
            event.upstream,
            event.old_state.value,
            event.new_state.value,
            event.reason,
            event.consecutive_failures,
        )
        if self._on_event is not None:
            self._on_event(event)

    async def record_success(self, route: str, upstream: str) -> None:
        key = self._key(route, upstream)
        async with self._locks[key]:
            old = self._states.get(key, HealthState.HEALTHY)
            self._failures[key] = 0
            self._ejected_at.pop(key, None)
            self._states[key] = HealthState.HEALTHY
            if old != HealthState.HEALTHY:
                self._emit(key, old, HealthState.HEALTHY, "probe_succeeded")

    async def record_failure(self, route: str, upstream: str, *, reason: str = "") -> bool:
        """Returns True iff this call ejected (or re-ejected) the instance."""
        key = self._key(route, upstream)
        async with self._locks[key]:
            state = self._states.get(key, HealthState.HEALTHY)
            if state == HealthState.PROBING:
                if self._failures[key] + 1 >= self._unhealthy_threshold:
                    # Recovery probe failed while failure budget is exhausted:
                    # re-eject and restart the cooldown.
                    self._failures[key] += 1
                    self._ejected_at[key] = self._clock()
                    self._states[key] = HealthState.UNHEALTHY
                    self._emit(
                        key, HealthState.PROBING, HealthState.UNHEALTHY, "probe_failed:" + reason
                    )
                    return True
                # Probe failed but the health budget is not exhausted: the
                # instance stays in rotation (breaker may still reject it).
                self._failures[key] += 1
                self._states[key] = HealthState.HEALTHY
                self._emit(
                    key, HealthState.PROBING, HealthState.HEALTHY, "probe_failed_in_rotation"
                )
                return False
            if state == HealthState.UNHEALTHY:
                return False
            self._failures[key] += 1
            if self._failures[key] >= self._unhealthy_threshold:
                self._ejected_at[key] = self._clock()
                self._states[key] = HealthState.UNHEALTHY
                self._emit(key, HealthState.HEALTHY, HealthState.UNHEALTHY, "threshold:" + reason)
                return True
            return False

    async def release_probe(
        self, route: str, upstream: str, *, reset_cooldown: bool = False
    ) -> None:
        """Give back a claimed probe slot without sending anything.

        Used when an outer component (e.g. the circuit breaker) refuses the
        request. By default the original ejection time is kept; with
        ``reset_cooldown`` a fresh cooldown window starts.
        """
        key = self._key(route, upstream)
        async with self._locks[key]:
            if self._states.get(key) != HealthState.PROBING:
                return
            if reset_cooldown:
                self._ejected_at[key] = self._clock()
            self._states[key] = HealthState.UNHEALTHY

    async def acquire_probe(self, route: str, upstream: str, *, now: float | None = None) -> bool:
        """Atomically claim the single recovery probe slot."""
        key = self._key(route, upstream)
        async with self._locks[key]:
            state = self._states.get(key, HealthState.HEALTHY)
            if state != HealthState.UNHEALTHY:
                return False
            if now is None:
                now = self._clock()
            if now - self._ejected_at.get(key, 0.0) < self._cooldown:
                return False
            self._states[key] = HealthState.PROBING
            logger.info(
                "health %s/%s: unhealthy -> probing (recovery probe admitted)",
                route,
                upstream,
            )
            return True


class LoadBalancer:
    """Round-robin selection among currently available instances.

    Healthy instances are always preferred; only when none are healthy does
    the balancer admit at most one recovery probe (atomically claimed) to an
    instance whose cooldown has elapsed.
    """

    def __init__(self, tracker: HealthTracker) -> None:
        self._tracker = tracker
        self._counters: dict[str, int] = defaultdict(int)
        self._counter_lock = asyncio.Lock()

    async def _next_index(self, route: str, size: int) -> int:
        async with self._counter_lock:
            idx = self._counters[route] % size
            self._counters[route] += 1
            return idx

    def reset_counters(self) -> None:
        self._counters.clear()

    async def select(
        self,
        route: str,
        upstreams: tuple[UpstreamConfig, ...],
        *,
        exclude: frozenset[str] = frozenset(),
        allow_probe: bool = True,
    ) -> Selection | None:
        now = self._tracker._clock()
        pool = [u for u in upstreams if u.name not in exclude]

        # 1) A cooled-down ejected instance is admitted as a single recovery
        #    probe before normal traffic. This re-includes recovered instances
        #    promptly; a failed probe can be retried on a healthy peer by the
        #    handler. Probing is skipped for requests that cannot be retried
        #    safely when a healthy peer exists, so they never waste their only
        #    attempt on a likely-failing probe.
        if allow_probe:
            cooled = [
                u
                for u in pool
                if self._tracker.state_of(route, u.name) == HealthState.UNHEALTHY
                and self._tracker.is_available(route, u.name, now=now)
            ]
            if cooled:
                start = await self._next_index(route, len(cooled))
                for offset in range(len(cooled)):
                    candidate = cooled[(start + offset) % len(cooled)]
                    if await self._tracker.acquire_probe(route, candidate.name, now=now):
                        return Selection(candidate, is_probe=True)

        # 2) Otherwise round-robin among healthy instances.
        healthy = [
            u
            for u in pool
            if self._tracker.state_of(route, u.name) == HealthState.HEALTHY
        ]
        if healthy:
            idx = await self._next_index(route, len(healthy))
            return Selection(healthy[idx], is_probe=False)

        return None
