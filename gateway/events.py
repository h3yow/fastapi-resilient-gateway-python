"""健康变化与熔断状态迁移的事件记录。

同时写入：
1. 标准 logger（``gateway``）——单测运行时可直接在日志里看到；
2. 内存环形缓冲——供诊断端点与断言使用，保证结论稳定、可复现。
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field

logger = logging.getLogger("gateway")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)


@dataclass(frozen=True)
class Event:
    time: float
    kind: str
    target: str
    message: str


@dataclass
class EventLog:
    capacity: int = 1000
    _events: deque[Event] = field(init=False)

    def __post_init__(self) -> None:
        self._events = deque(maxlen=self.capacity)

    def emit(self, kind: str, target: str, message: str, *, time: float) -> None:
        event = Event(time=time, kind=kind, target=target, message=message)
        self._events.append(event)
        logger.info("event=%s target=%s | %s", kind, target, message)

    def events(self) -> list[Event]:
        return list(self._events)

    def of_kind(self, kind: str) -> list[Event]:
        return [e for e in self._events if e.kind == kind]

    def reset(self) -> None:
        self._events.clear()
