"""Deterministic fakes used by the gateway test-suite."""

import asyncio
from collections import defaultdict, deque

from gateway.transport import (
    UpstreamConnectionError,
    UpstreamRequest,
    UpstreamResponse,
    UpstreamTimeout,
)


class ScriptedTransport:
    """Transport whose behavior per base_url is scripted per call.

    behaviors[url] is a deque of either:
      int                -> HTTP status response
      "timeout"          -> raise UpstreamTimeout
      "unreachable"      -> raise UpstreamConnectionError
      callable(req, idx) -> UpstreamResponse | str
    When the deque is exhausted the last entry repeats.
    """

    def __init__(self) -> None:
        self.behaviors: dict[str, deque] = defaultdict(deque)
        self.calls: dict[str, list[UpstreamRequest]] = defaultdict(list)

    def set(self, base_url: str, *entries) -> None:
        self.behaviors[base_url] = deque(entries)

    async def send(self, base_url: str, request: UpstreamRequest, *, timeout: float) -> UpstreamResponse:
        await asyncio.sleep(0)
        self.calls[base_url].append(request)
        q = self.behaviors[base_url]
        entry = q[0]
        if len(q) > 1:
            q.popleft()  # keep the final entry repeating forever
        idx = len(self.calls[base_url]) - 1
        if callable(entry):
            result = entry(request, idx)
            if isinstance(result, UpstreamResponse):
                return result
            entry = result
        if entry == "timeout":
            raise UpstreamTimeout("scripted timeout")
        if entry == "unreachable":
            raise UpstreamConnectionError("scripted unreachable")
        return UpstreamResponse(
            status_code=int(entry),
            body=f"{base_url}:{idx}".encode(),
            upstream_name="",
        )


class ManualClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class InstantSleep:
    """Sleep that records delays but does not wait."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        await asyncio.sleep(0)
