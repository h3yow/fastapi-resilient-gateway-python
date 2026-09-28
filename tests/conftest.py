"""测试公共设施：可控时钟、内存 FakeTransport、标准网关构造。"""

from __future__ import annotations

import random
from collections.abc import Awaitable, Callable

import pytest

from gateway.config import GatewayConfig, load_config
from gateway.events import EventLog
from gateway.proxy import Gateway
from gateway.transport import UpstreamRequest, UpstreamResponse

SendBehavior = Callable[[str, UpstreamRequest], Awaitable[UpstreamResponse]]


class ManualClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class FakeTransport:
    """按 base_url 分发的内存传输，可分别控制 send 行为与健康探测结果。"""

    def __init__(self, behavior: dict[str, SendBehavior]) -> None:
        self.behavior = behavior
        # base_url -> 是否健康（主动探测结果）
        self.healthy: dict[str, bool] = {}
        # 调用记录：(base_url, method, path, idempotency_key, body)
        self.calls: list[tuple[str, str, str, str | None, bytes]] = []
        self.probe_calls: list[str] = []

    async def send(self, base_url: str, request: UpstreamRequest) -> UpstreamResponse:
        self.calls.append(
            (
                base_url,
                request.method,
                request.path,
                request.headers.get("idempotency-key"),
                request.body,
            )
        )
        fn = self.behavior[base_url]
        return await fn(base_url, request)

    async def health_check(self, base_url: str, path: str, timeout: float = 1.0) -> bool:
        self.probe_calls.append(base_url)
        return self.healthy.get(base_url, False)

    async def aclose(self) -> None:
        return None


def build_config(**overrides) -> GatewayConfig:
    upstream_defaults = {
        "failure_threshold": 3,
        "recover_successes": 2,
        "probe_interval": 0.5,
    }
    route_retry = overrides.pop("retry", {"max_attempts": 3, "base_delay": 0.0, "jitter": 0.0})
    route_circuit = overrides.pop(
        "circuit",
        {"failure_threshold": 3, "open_cooldown": 1.0, "half_open_max_probes": 1},
    )
    data = {
        "upstreams": [
            {"id": "a", "base_url": "http://a", **upstream_defaults},
            {"id": "b", "base_url": "http://b", **upstream_defaults},
        ],
        "routes": [
            {
                "id": "r",
                "prefix": "/api",
                "upstream_ids": ["a", "b"],
                "timeout": 1.0,
                "retry": route_retry,
                "circuit": route_circuit,
                **overrides,
            }
        ],
    }
    return load_config(data)


def make_gateway(config: GatewayConfig, transport: FakeTransport, clock: ManualClock,
                 event_log: EventLog | None = None) -> Gateway:
    log = event_log or EventLog()
    return Gateway(
        config,
        transport,
        event_log=log,
        clock=clock,
        rng=random.Random(42),
        sleep=_no_sleep,
    )


async def _no_sleep(_delay: float) -> None:
    return None


def ok_response(status: int = 200, body: bytes = b"ok") -> Callable:
    async def fn(base_url: str, request: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status, body=body)

    return fn


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def event_log() -> EventLog:
    log = EventLog()
    return log
