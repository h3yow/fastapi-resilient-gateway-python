"""可替换的时钟与睡眠，便于测试中控制时间。"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        return time.monotonic()


async def system_sleep(delay: float) -> None:
    await asyncio.sleep(delay)
