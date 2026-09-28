"""熔断器：开合转换、半开探测、冷却拒绝、并发触发不产生抖动/误判。"""

from __future__ import annotations

import asyncio

import pytest

from gateway.circuit import CircuitBreaker, CircuitState
from gateway.config import CircuitConfig
from gateway.events import EventLog


def make_cb(clock, **cfg) -> CircuitBreaker:
    return CircuitBreaker(
        "r:a",
        CircuitConfig(**cfg),
        EventLog(),
        clock,
    )


def test_closed_to_open_after_threshold(clock):
    cb = make_cb(clock, failure_threshold=3, open_cooldown=10.0)
    cb.record_failure()
    cb.record_failure()
    assert cb.state is CircuitState.CLOSED
    cb.record_failure()
    assert cb.state is CircuitState.OPEN
    # 冷却期内一律拒绝
    assert cb.allow_request() is False
    clock.advance(9.9)
    assert cb.allow_request() is False


def test_half_open_success_recovers(clock):
    cb = make_cb(clock, failure_threshold=1, open_cooldown=5.0, half_open_successes=1)
    cb.record_failure()
    assert cb.state is CircuitState.OPEN
    clock.advance(5.0)
    assert cb.allow_request() is True
    assert cb.state is CircuitState.HALF_OPEN
    cb.record_success()
    assert cb.state is CircuitState.CLOSED
    assert cb.allow_request() is True


def test_half_open_failure_reopens_and_resets_cooldown(clock):
    cb = make_cb(clock, failure_threshold=1, open_cooldown=5.0)
    cb.record_failure()
    clock.advance(5.0)
    assert cb.allow_request() is True  # 进入半开
    cb.record_failure()  # 探测失败
    assert cb.state is CircuitState.OPEN
    # 冷却被重置：刚过原冷却点仍不可放行
    clock.advance(4.9)
    assert cb.allow_request() is False
    clock.advance(0.1)
    assert cb.allow_request() is True  # 新冷却结束 -> 再次半开
    assert cb.state is CircuitState.HALF_OPEN


def test_half_open_limits_concurrent_probes(clock):
    cb = make_cb(
        clock,
        failure_threshold=1,
        open_cooldown=5.0,
        half_open_max_probes=2,
        half_open_successes=2,
    )
    cb.record_failure()
    clock.advance(5.0)
    assert cb.allow_request() is True   # 探测名额 1
    assert cb.allow_request() is True   # 探测名额 2
    assert cb.allow_request() is False  # 超出并发，按降级处理
    cb.record_success()
    assert cb.state is CircuitState.HALF_OPEN  # 仍需一次成功
    # 成功一次后释放一个名额，后续探测可补上
    assert cb.allow_request() is True
    assert cb.allow_request() is False
    cb.record_success()
    assert cb.state is CircuitState.CLOSED


def test_state_transitions_logged(clock):
    log = EventLog()
    cb = CircuitBreaker("r:a", CircuitConfig(failure_threshold=1, open_cooldown=1.0), log, clock)
    cb.record_failure()
    clock.advance(1.0)
    cb.allow_request()
    cb.record_success()
    msgs = [e.message for e in log.of_kind("circuit_state")]
    assert any("closed -> open" in m for m in msgs)
    assert any("open -> half_open" in m for m in msgs)
    assert any("half_open -> closed" in m for m in msgs)


def test_concurrent_failures_do_not_lose_count_or_double_transition(clock):
    """并发记账：未熔断时失败计数不能丢；熔断瞬间迁移事件恰好一次。"""
    import threading

    log = EventLog()
    threads = 10
    per_thread = 50

    # 场景一：阈值足够大，500 个并发失败全部入账，状态不迁移
    cb = CircuitBreaker("r:a", CircuitConfig(failure_threshold=1000, open_cooldown=10.0), log, clock)
    workers = [
        threading.Thread(
            target=lambda cb=cb: [cb.record_failure() for _ in range(per_thread)]
        )
        for _ in range(threads)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    assert cb.snapshot()["consecutive_failures"] == threads * per_thread
    assert cb.state is CircuitState.CLOSED

    # 场景二：阈值 250，500 个并发失败 -> 恰好迁移一次 OPEN
    cb2 = CircuitBreaker("r:b", CircuitConfig(failure_threshold=250, open_cooldown=10.0), log, clock)
    workers = [
        threading.Thread(
            target=lambda cb=cb2: [cb.record_failure() for _ in range(per_thread)]
        )
        for _ in range(threads)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    assert cb2.state is CircuitState.OPEN
    opens = [
        e
        for e in log.of_kind("circuit_state")
        if e.target == "r:b" and "closed -> open" in e.message
    ]
    assert len(opens) == 1  # 恰好迁移一次，无抖动


@pytest.mark.asyncio
async def test_concurrent_requests_coalesce_single_half_open_probe(clock):
    """冷却到期瞬间并发 N 个请求，只有 half_open_max_probes 个能进入探测。"""
    cb = make_cb(
        clock,
        failure_threshold=1,
        open_cooldown=5.0,
        half_open_max_probes=1,
    )
    cb.record_failure()
    clock.advance(5.0)

    results = await asyncio.gather(
        *[asyncio.to_thread(cb.allow_request) for _ in range(50)]
    )
    assert sum(1 for r in results if r) == 1
    assert sum(1 for r in results if not r) == 49
    assert cb.state is CircuitState.HALF_OPEN
