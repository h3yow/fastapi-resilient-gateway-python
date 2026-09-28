"""带幂等标识写请求的去重存储（内存、线程安全）。

语义：
- 首次携带 Idempotency-Key 的写请求进入 IN_FLIGHT 占位；
- 同一时刻相同 key 的并发请求返回 409（IDEMPOTENCY_CONFLICT），
  避免上游被重复执行产生重复副作用；
- 请求完成后缓存响应（DONE），同 key 重放直接返回首次结果，网关重试
  期间 key 始终占位，上游不会因重试收到重复提交；
- 上游最终失败（重试耗尽）时调用 fail 释放 key，允许客户端带同一 key
  重新发起（之前的提交未成功，不会有重复副作用）；
- DONE 记录带 TTL，惰性过期。
"""

from __future__ import annotations

import enum
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from gateway.errors import FailureReason, GatewayError

if TYPE_CHECKING:
    from gateway.clock import Clock


class IdemStatus(str, enum.Enum):
    IN_FLIGHT = "in_flight"
    DONE = "done"


class IdempotencyConflict(GatewayError):
    def __init__(self, message: str = "idempotency key is being processed") -> None:
        super().__init__(FailureReason.IDEMPOTENCY_CONFLICT, message, status_code=409)


@dataclass
class IdempotencyRecord:
    status: IdemStatus
    status_code: int = 0
    body: bytes = b""
    headers: dict[str, list[str]] = field(default_factory=dict)
    expires_at: float = 0.0


class IdempotencyStore:
    def __init__(self, clock: Clock, ttl: float = 300.0) -> None:
        self._clock = clock
        self._ttl = ttl
        self._records: dict[str, IdempotencyRecord] = {}
        self._lock = threading.Lock()

    def acquire(self, key: str) -> IdempotencyRecord | None:
        """尝试占位。

        - 命中已完成且未过期的记录 -> 返回该记录（调用方直接回放）；
        - 命中进行中的记录 -> 抛 IdempotencyConflict；
        - 占位成功 -> 返回 None。
        """
        with self._lock:
            now = self._clock.now()
            rec = self._records.get(key)
            if rec is not None:
                if rec.status is IdemStatus.DONE:
                    if rec.expires_at <= now:
                        del self._records[key]
                    else:
                        return rec
                else:
                    raise IdempotencyConflict()
            self._records[key] = IdempotencyRecord(status=IdemStatus.IN_FLIGHT)
            return None

    def complete(
        self,
        key: str,
        status_code: int,
        body: bytes,
        headers: dict[str, list[str]],
    ) -> None:
        with self._lock:
            self._records[key] = IdempotencyRecord(
                status=IdemStatus.DONE,
                status_code=status_code,
                body=body,
                headers=dict(headers),
                expires_at=self._clock.now() + self._ttl,
            )

    def fail(self, key: str) -> None:
        """上游最终失败：释放占位，允许同 key 重新提交。"""
        with self._lock:
            self._records.pop(key, None)

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return {k: v.status.value for k, v in self._records.items()}
