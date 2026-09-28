"""上游实例健康状态跟踪：被动失败摘除 + 主动探测恢复。

规则（每个上游独立）：
- 初始 HEALTHY；被动记录连续失败，达到 ``failure_threshold`` 前为 SUSPECT，
  SUSPECT 仍承接流量（给实例短暂抖动的机会）；
- 连续失败达到阈值 -> UNHEALTHY，负载均衡立即摘除，请求不再打到该实例；
- UNHEALTHY 后由后台探测任务按 ``probe_interval`` 主动探活，连续
  ``recover_successes`` 次探测成功才恢复 HEALTHY 并重新纳入；
- 任意一次被动成功都会清空失败计数并恢复 HEALTHY。

所有状态迁移都会写入 EventLog（kind=health_changed / health_probe）。
锁内不做 await，并发记账不会丢更新。
"""

from __future__ import annotations

import enum
import logging
import threading

from gateway.clock import Clock
from gateway.config import UpstreamConfig
from gateway.events import EventLog
from gateway.transport import Transport

_log = logging.getLogger("gateway")


class HealthState(str, enum.Enum):
    HEALTHY = "healthy"
    SUSPECT = "suspect"
    UNHEALTHY = "unhealthy"


class _State:
    __slots__ = (
        "failures",
        "last_probe_at",
        "probe_successes",
        "state",
    )

    def __init__(self) -> None:
        self.state: HealthState = HealthState.HEALTHY
        self.failures = 0
        self.probe_successes = 0
        self.last_probe_at: float | None = None


class HealthTracker:
    def __init__(
        self,
        upstreams: dict[str, UpstreamConfig],
        event_log: EventLog,
        clock: Clock,
    ) -> None:
        self._configs = upstreams
        self._log = event_log
        self._clock = clock
        self._states: dict[str, _State] = {uid: _State() for uid in upstreams}
        self._lock = threading.Lock()

    # ---- 查询 -----------------------------------------------------------
    def is_healthy(self, upstream_id: str) -> bool:
        """HEALTHY 与 SUSPECT 都承接流量；只有 UNHEALTHY 被摘除。"""
        with self._lock:
            return self._states[upstream_id].state is not HealthState.UNHEALTHY

    def healthy_ids(self, upstream_ids: list[str]) -> list[str]:
        """按给定顺序返回未被摘除的实例（顺序稳定，便于轮询复现）。"""
        with self._lock:
            return [uid for uid in upstream_ids if self._states[uid].state is not HealthState.UNHEALTHY]

    def state_of(self, upstream_id: str) -> HealthState:
        with self._lock:
            return self._states[upstream_id].state

    # ---- 被动记账 -------------------------------------------------------
    def record_success(self, upstream_id: str) -> None:
        with self._lock:
            st = self._states[upstream_id]
            if st.state is HealthState.HEALTHY and st.failures == 0:
                return
            old = st.state
            st.state = HealthState.HEALTHY
            st.failures = 0
            st.probe_successes = 0
            if old is not HealthState.HEALTHY:
                self._emit_change(upstream_id, old, HealthState.HEALTHY, "passive success")

    def record_failure(self, upstream_id: str) -> HealthState:
        with self._lock:
            st = self._states[upstream_id]
            st.failures += 1
            threshold = self._configs[upstream_id].failure_threshold
            new = HealthState.UNHEALTHY if st.failures >= threshold else HealthState.SUSPECT
            old = st.state
            if new is not old:
                st.state = new
                if new is HealthState.UNHEALTHY:
                    # 允许立即发起第一次探测
                    st.probe_successes = 0
                    st.last_probe_at = None
                self._emit_change(
                    upstream_id,
                    old,
                    new,
                    f"{st.failures} consecutive failure(s), threshold={threshold}",
                )
            return st.state

    # ---- 主动探测 -------------------------------------------------------
    def probe_due(self, upstream_id: str) -> bool:
        with self._lock:
            st = self._states[upstream_id]
            if st.state is not HealthState.UNHEALTHY:
                return False
            if st.last_probe_at is None:
                return True
            return self._clock.now() - st.last_probe_at >= self._configs[upstream_id].probe_interval

    async def probe(self, upstream_id: str, transport: Transport) -> bool:
        """对 UNHEALTHY 实例执行一次主动探测；返回探测是否成功。"""
        with self._lock:
            st = self._states[upstream_id]
            if st.state is not HealthState.UNHEALTHY:
                return False
            st.last_probe_at = self._clock.now()

        cfg = self._configs[upstream_id]
        try:
            ok = await transport.health_check(cfg.base_url, cfg.probe_path)
        except Exception as exc:  # noqa: BLE001 - 探测异常等价于失败，必须吞掉保证循环存活
            _log.warning("probe %s raised: %r", upstream_id, exc)
            ok = False

        with self._lock:
            st = self._states[upstream_id]
            if st.state is not HealthState.UNHEALTHY:
                return False
            if ok:
                st.probe_successes += 1
                self._log.emit(
                    "health_probe",
                    upstream_id,
                    f"probe ok ({st.probe_successes}/{cfg.recover_successes})",
                    time=self._clock.now(),
                )
                if st.probe_successes >= cfg.recover_successes:
                    old = st.state
                    st.state = HealthState.HEALTHY
                    st.failures = 0
                    self._emit_change(upstream_id, old, HealthState.HEALTHY, "probe recovery")
                return True
            st.probe_successes = 0
            self._log.emit(
                "health_probe",
                upstream_id,
                "probe failed, staying unhealthy",
                time=self._clock.now(),
            )
            return False

    # ---- 诊断 -----------------------------------------------------------
    def snapshot(self) -> dict[str, dict[str, object]]:
        with self._lock:
            return {
                uid: {
                    "state": st.state.value,
                    "consecutive_failures": st.failures,
                    "probe_successes": st.probe_successes,
                }
                for uid, st in self._states.items()
            }

    def _emit_change(self, uid: str, old: HealthState, new: HealthState, why: str) -> None:
        self._log.emit(
            "health_changed",
            uid,
            f"{old.value} -> {new.value} ({why})",
            time=self._clock.now(),
        )
