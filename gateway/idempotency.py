"""In-memory idempotency store preventing duplicate side effects.

A request carrying an ``Idempotency-Key`` is executed at most once: concurrent
replays share the same in-flight execution and later replays receive the stored
response. The key is also forwarded upstream so an upstream aware of it can
suppress duplicate side effects on retries.

Only successful upstream responses (any HTTP status that was actually served)
are stored; after a transport-level failure the key is released so the client
is free to retry.
"""

import asyncio
from collections import defaultdict
from dataclasses import dataclass

from gateway.transport import UpstreamResponse


@dataclass
class IdempotencyOutcome:
    status: str  # "new" | "in_flight" | "replay"
    stored: UpstreamResponse | None = None
    future: "asyncio.Future[UpstreamResponse] | None" = None


class IdempotencyStore:
    def __init__(self) -> None:
        self._responses: dict[str, UpstreamResponse] = {}
        self._futures: dict[str, asyncio.Future[UpstreamResponse]] = {}
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def begin(self, key: str | None) -> IdempotencyOutcome:
        if key is None:
            return IdempotencyOutcome(status="new")
        async with self._locks[key]:
            if key in self._responses:
                return IdempotencyOutcome(status="replay", stored=self._responses[key])
            if key in self._futures:
                return IdempotencyOutcome(status="in_flight", future=self._futures[key])
            future: asyncio.Future[UpstreamResponse] = asyncio.get_running_loop().create_future()
            self._futures[key] = future
            return IdempotencyOutcome(status="new", future=future)

    async def wait_in_flight(self, future: "asyncio.Future[UpstreamResponse]") -> UpstreamResponse:
        return await future

    async def complete(self, key: str, response: UpstreamResponse) -> None:
        async with self._locks[key]:
            future = self._futures.pop(key, None)
            self._responses[key] = response
        # Resolve waiters outside the lock: waking coroutines may re-enter
        # begin() and need the same lock.
        if future is not None and not future.done():
            future.set_result(response)

    async def fail(self, key: str) -> None:
        """Release the key after a failure so the client may retry."""
        async with self._locks[key]:
            future = self._futures.pop(key, None)
        if future is not None and not future.done():
            future.cancel()
