"""Per (route, upstream) circuit breaker: closed / open / half-open."""

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Callable

logger = logging.getLogger("gateway.breaker")


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class BreakerEvent:
    route: str
    upstream: str
    old_state: BreakerState
    new_state: BreakerState
    reason: str
    consecutive_failures: int


EventHandler = Callable[[BreakerEvent], None]


@dataclass(frozen=True)
class BreakerDecision:
    allowed: bool
    state: BreakerState
    is_probe: bool


def _monotonic() -> float:
    return time.monotonic()


class CircuitBreaker:
    """Concurrency-safe circuit state machine.

    CLOSED: all requests pass; ``failure_threshold`` consecutive failures open.
    OPEN: all requests fast-fail (degraded); after ``open_cooldown`` the first
          request atomically enters HALF_OPEN as the single probe.
    HALF_OPEN: only the probe runs; success -> CLOSED, failure -> OPEN (with a
          fresh cooldown). Concurrent requests while the probe is in flight are
          fast-failed, so the state cannot be toggled back and forth.
    """

    def __init__(
        self,
        failure_threshold: int,
        open_cooldown: float,
        *,
        clock: Callable[[], float] | None = None,
        on_event: EventHandler | None = None,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._open_cooldown = open_cooldown
        self._clock = clock or _monotonic
        self._on_event = on_event
        self._states: dict[tuple[str, str], BreakerState] = {}
        self._failures: dict[tuple[str, str], int] = defaultdict(int)
        self._opened_at: dict[tuple[str, str], float] = {}
        self._locks: dict[tuple[str, str], asyncio.Lock] = defaultdict(asyncio.Lock)

    def _key(self, route: str, upstream: str) -> tuple[str, str]:
        return (route, upstream)

    def state_of(self, route: str, upstream: str) -> BreakerState:
        return self._states.get(self._key(route, upstream), BreakerState.CLOSED)

    def _emit(
        self,
        key: tuple[str, str],
        old: BreakerState,
        new: BreakerState,
        reason: str,
    ) -> None:
        event = BreakerEvent(
            route=key[0],
            upstream=key[1],
            old_state=old,
            new_state=new,
            reason=reason,
            consecutive_failures=self._failures[key],
        )
        logger.info(
            "breaker %s/%s: %s -> %s (%s, failures=%d)",
            event.route,
            event.upstream,
            event.old_state.value,
            event.new_state.value,
            event.reason,
            event.consecutive_failures,
        )
        if self._on_event is not None:
            self._on_event(event)

    async def allow(self, route: str, upstream: str, *, now: float | None = None) -> BreakerDecision:
        key = self._key(route, upstream)
        async with self._locks[key]:
            state = self._states.get(key, BreakerState.CLOSED)
            if state == BreakerState.CLOSED:
                return BreakerDecision(True, BreakerState.CLOSED, False)
            if state == BreakerState.HALF_OPEN:
                # Probe already in flight: do not let concurrent traffic through.
                return BreakerDecision(False, BreakerState.HALF_OPEN, False)
            # OPEN: cooldown elapsed?
            if now is None:
                now = self._clock()
            if now - self._opened_at.get(key, 0.0) >= self._open_cooldown:
                self._states[key] = BreakerState.HALF_OPEN
                logger.info(
                    "breaker %s/%s: open -> half_open (probe admitted)", route, upstream
                )
                return BreakerDecision(True, BreakerState.HALF_OPEN, True)
            return BreakerDecision(False, BreakerState.OPEN, False)

    async def record_success(self, route: str, upstream: str) -> None:
        key = self._key(route, upstream)
        async with self._locks[key]:
            old = self._states.get(key, BreakerState.CLOSED)
            self._failures[key] = 0
            self._opened_at.pop(key, None)
            self._states[key] = BreakerState.CLOSED
            if old != BreakerState.CLOSED:
                self._emit(key, old, BreakerState.CLOSED, "probe_succeeded")

    async def record_failure(self, route: str, upstream: str, *, reason: str = "") -> None:
        key = self._key(route, upstream)
        async with self._locks[key]:
            state = self._states.get(key, BreakerState.CLOSED)
            if state == BreakerState.HALF_OPEN:
                # Probe failed: re-open immediately with a fresh cooldown.
                self._opened_at[key] = self._clock()
                self._failures[key] = self._failure_threshold
                self._states[key] = BreakerState.OPEN
                self._emit(key, BreakerState.HALF_OPEN, BreakerState.OPEN, "probe_failed:" + reason)
                return
            if state == BreakerState.OPEN:
                return
            self._failures[key] += 1
            if self._failures[key] >= self._failure_threshold:
                self._opened_at[key] = self._clock()
                self._states[key] = BreakerState.OPEN
                self._emit(key, BreakerState.CLOSED, BreakerState.OPEN, "threshold:" + reason)
