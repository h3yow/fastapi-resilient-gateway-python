"""基础单元：错误归一化、轮询、重试策略。"""

from __future__ import annotations

import random

import pytest

from gateway.balancer import RoundRobinBalancer
from gateway.config import RetryConfig
from gateway.errors import FailureReason, classify_http_status
from gateway.retry import compute_delay, is_method_retryable


@pytest.mark.parametrize(
    "status,reason,retryable",
    [
        (400, FailureReason.UPSTREAM_HTTP, False),
        (404, FailureReason.UPSTREAM_HTTP, False),
        (401, FailureReason.UPSTREAM_UNAUTHORIZED, False),
        (408, FailureReason.UPSTREAM_TIMEOUT, True),
        (429, FailureReason.UPSTREAM_RATE_LIMITED, True),
        (500, FailureReason.UPSTREAM_SERVER, True),
        (502, FailureReason.UPSTREAM_SERVER, True),
        (503, FailureReason.UPSTREAM_SERVER, True),
        (504, FailureReason.UPSTREAM_SERVER, True),
        (501, FailureReason.UPSTREAM_SERVER, False),
    ],
)
def test_status_classification(status, reason, retryable):
    assert classify_http_status(status) == (reason, retryable)


def test_round_robin_balances_over_healthy_only():
    b = RoundRobinBalancer()
    order = ["a", "b", "c"]
    picks = [b.choose("r", order, ["a", "b", "c"]) for _ in range(3)]
    assert picks == ["a", "b", "c"]
    # b 摘除：只在 a,c 间轮询（计数器连续，结果确定）
    assert [b.choose("r", order, ["a", "c"]) for _ in range(2)] == ["c", "a"]
    # b 恢复后继续参与
    assert b.choose("r", order, ["a", "b", "c"]) == "c"
    assert b.choose("r", order, []) is None


def test_retry_method_safety():
    cfg = RetryConfig()
    for m in ("GET", "HEAD", "OPTIONS", "PUT", "DELETE"):
        assert is_method_retryable(m, cfg)
    assert not is_method_retryable("POST", cfg)
    assert is_method_retryable("POST", RetryConfig(retry_post=True))
    assert is_method_retryable("POST", cfg, has_idempotency_key=True)
    assert not is_method_retryable("PATCH", cfg)


def test_backoff_exponential_with_jitter_bounds():
    cfg = RetryConfig(base_delay=0.1, max_delay=1.0, jitter=0.2)
    rng = random.Random(1)
    d1 = compute_delay(1, cfg, rng)
    d2 = compute_delay(2, cfg, rng)
    d3 = compute_delay(3, cfg, rng)
    assert 0.08 <= d1 <= 0.12
    assert 0.16 <= d2 <= 0.24
    assert 0.32 <= d3 <= 0.48
    # 上限
    big = compute_delay(20, cfg, rng)
    assert big <= 1.2
