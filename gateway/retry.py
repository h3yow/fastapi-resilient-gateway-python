"""重试策略：只对可安全重试的失败重试，带次数上限与退避抖动。

- 方法安全性：GET/HEAD/OPTIONS/PUT/DELETE 天然幂等可重试；POST 默认不
  重试，除非路由显式开启 retry_post（要求上游自身保证幂等，或请求带
  Idempotency-Key 时由代理层判定为安全）。
- 退避：delay = min(max_delay, base_delay * 2**(attempt-1))，再叠加
  [1-jitter, 1+jitter] 区间的乘法抖动，避免重试风暴。
"""

from __future__ import annotations

import random
from collections.abc import Awaitable, Callable

from gateway.config import RetryConfig

_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})


def is_method_retryable(
    method: str,
    config: RetryConfig,
    *,
    has_idempotency_key: bool = False,
) -> bool:
    m = method.upper()
    if m in _SAFE_METHODS:
        return True
    if m == "POST":
        return config.retry_post or has_idempotency_key
    return False


def compute_delay(
    attempt: int,
    config: RetryConfig,
    rng: random.Random | None = None,
) -> float:
    """第 attempt 次失败后的退避时长（attempt 从 1 开始）。"""
    rng = rng or random
    raw = min(config.max_delay, config.base_delay * (2 ** (attempt - 1)))
    factor = rng.uniform(1.0 - config.jitter, 1.0 + config.jitter)
    return max(0.0, raw * factor)


Sleep = Callable[[float], Awaitable[None]]


async def backoff_delay(
    attempt: int,
    config: RetryConfig,
    sleep: Sleep,
    rng: random.Random | None = None,
) -> float:
    """睡眠一个退避周期并返回实际时长。"""
    delay = compute_delay(attempt, config, rng)
    await sleep(delay)
    return delay
