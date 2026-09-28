"""Retry policy: safety classification, budgets and jittered backoff."""

import random
from dataclasses import dataclass
from typing import Callable

IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})


def is_retriable_status(status: int) -> bool:
    """5xx responses are retriable; 4xx is a client error and never retried."""
    return status >= 500


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3          # total attempts including the first
    base_delay: float = 0.05       # seconds
    max_delay: float = 1.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be non-negative")


def is_safe_to_retry(method: str, has_idempotency_key: bool) -> bool:
    """Only idempotent methods (or requests carrying an Idempotency-Key) retry.

    A non-idempotent POST/PATCH is retried only when the client supplies an
    Idempotency-Key, which guarantees upstream duplicate suppression.
    """
    return method.upper() in IDEMPOTENT_METHODS or has_idempotency_key


def compute_backoff(
    policy: RetryPolicy,
    attempt: int,
    *,
    random_func: Callable[[], float] | None = None,
) -> float:
    """Exponential backoff with *full jitter* before attempt ``attempt + 1``.

    delay = random(0, min(max_delay, base_delay * 2 ** attempt))
    With jitter disabled the deterministic exponential value is returned.
    """
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    exp = policy.base_delay * (2**attempt)
    cap = min(policy.max_delay, exp)
    if policy.jitter:
        rnd = random_func if random_func is not None else random.random
        return rnd() * cap
    return cap
