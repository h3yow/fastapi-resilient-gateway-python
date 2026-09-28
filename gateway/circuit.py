"""按 (路由, 上游) 维度的熔断器：CLOSED / OPEN / HALF_OPEN 状态机。

状态迁移：
- CLOSED：连续失败达到 ``failure_threshold`` -> OPEN（摘除，请求直接降级）；
- OPEN：``open_cooldown`` 冷却期内 allow_request=False；冷却到期后首个拿到
  探测名额的请求把状态推进 HALF_OPEN；
- HALF_OPEN：最多放行 ``half_open_max_probes`` 个并发探测请求；
  * 探测成功累计 ``half_open_successes`` -> CLOSED（恢复）；
  * 任意探测失败 -> 立即回到 OPEN 并重置冷却计时；
  * 其余并发请求在 HALF_OPEN 拿不到名额时直接拒绝（OPEN 语义降级）。

并发安全：所有状态判断与计数都在同一把锁内完成，锁内不 await，
并发请求不会把状态机来回抖动或重复迁移。迟到的记账若发现状态已被
其它探测推进，则忽略，避免误判。
"""

from __future__ import annotations

import enum
import threading

from gateway.clock import Clock
from gateway.config import CircuitConfig
from gateway.events import EventLog


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        key: str,
        config: CircuitConfig,
        event_log: EventLog,
        clock: Clock,
    ) -> None:
        self.key = key
        self._cfg = config
        self._log = event_log
        self._clock = clock
        self._lock = threading.Lock()
        self._state: CircuitState = CircuitState.CLOSED
        self._failures = 0
        self._opened_at: float = 0.0
        self._in_flight_probes = 0
        self._half_open_successes = 0

    # ---- 放行判定 -------------------------------------------------------
    def allow_request(self) -> bool:
        with self._lock:
            now = self._clock.now()
            if self._state is CircuitState.CLOSED:
                return True
            if self._state is CircuitState.OPEN:
                if now - self._opened_at < self._cfg.open_cooldown:
                    return False
                # 冷却到期：原子地进入半开并占用第一个探测名额
                self._transition(CircuitState.OPEN, CircuitState.HALF_OPEN, now, "cooldown elapsed")
                self._in_flight_probes = 1
                self._half_open_successes = 0
                return True
            # HALF_OPEN：限制并发探测数，多余请求直接按降级处理
            if self._in_flight_probes < self._cfg.half_open_max_probes:
                self._in_flight_probes += 1
                return True
            return False

    # ---- 结果记账 -------------------------------------------------------
    def record_success(self) -> None:
        with self._lock:
            now = self._clock.now()
            if self._state is CircuitState.CLOSED:
                self._failures = 0
                return
            if self._state is not CircuitState.HALF_OPEN:
                return  # 状态已被其它探测推进，忽略迟到结果
            self._in_flight_probes = max(0, self._in_flight_probes - 1)
            self._half_open_successes += 1
            if self._half_open_successes >= self._cfg.half_open_successes:
                self._failures = 0
                self._transition(CircuitState.HALF_OPEN, CircuitState.CLOSED, now, "probe succeeded")
            # 仍需更多探测：保持 HALF_OPEN，后续请求继续拿名额

    def record_failure(self) -> None:
        with self._lock:
            now = self._clock.now()
            if self._state is CircuitState.HALF_OPEN:
                # 探测失败：立即重新熔断并重置冷却
                self._in_flight_probes = 0
                self._half_open_successes = 0
                self._opened_at = now
                self._transition(CircuitState.HALF_OPEN, CircuitState.OPEN, now, "probe failed")
                return
            if self._state is not CircuitState.CLOSED:
                return
            self._failures += 1
            if self._failures >= self._cfg.failure_threshold:
                self._opened_at = now
                self._transition(
                    CircuitState.CLOSED,
                    CircuitState.OPEN,
                    now,
                    f"{self._failures} consecutive failures, threshold={self._cfg.failure_threshold}",
                )

    # ---- 诊断 -----------------------------------------------------------
    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "state": self._state.value,
                "consecutive_failures": self._failures,
                "half_open_successes": self._half_open_successes,
                "in_flight_probes": self._in_flight_probes,
            }

    def _transition(self, old: CircuitState, new: CircuitState, at: float, why: str) -> None:
        self._state = new
        self._log.emit(
            "circuit_state",
            self.key,
            f"{old.value} -> {new.value} ({why})",
            time=at,
        )


class CircuitRegistry:
    def __init__(self, event_log: EventLog, clock: Clock) -> None:
        self._log = event_log
        self._clock = clock
        self._breakers: dict[tuple[str, str], CircuitBreaker] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _key(route_id: str, upstream_id: str) -> str:
        return f"{route_id}:{upstream_id}"

    def get(self, route_id: str, upstream_id: str, config: CircuitConfig) -> CircuitBreaker:
        pair = (route_id, upstream_id)
        with self._lock:
            cb = self._breakers.get(pair)
            if cb is None:
                cb = CircuitBreaker(self._key(route_id, upstream_id), config, self._log, self._clock)
                self._breakers[pair] = cb
            return cb

    def states(self) -> dict[str, dict[str, object]]:
        with self._lock:
            return {self._key(r, u): cb.snapshot() for (r, u), cb in self._breakers.items()}
