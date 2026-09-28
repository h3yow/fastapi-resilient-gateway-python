"""HTTP transport abstraction used to talk to upstream instances."""

from typing import Protocol

import httpx

from gateway.errors import FailureReason


class TransportError(Exception):
    """Base class for transport-level upstream failures."""

    reason: FailureReason = FailureReason.UPSTREAM_UNREACHABLE


class UpstreamTimeout(TransportError):
    reason = FailureReason.UPSTREAM_TIMEOUT


class UpstreamConnectionError(TransportError):
    reason = FailureReason.UPSTREAM_UNREACHABLE


class UpstreamRequest:
    def __init__(
        self,
        method: str = "GET",
        path: str = "/",
        headers: dict[str, str] | None = None,
        body: bytes = b"",
    ) -> None:
        self.method = method.upper()
        self.path = path
        # HTTP header field names are case-insensitive: normalize to lowercase.
        self.headers = {k.lower(): v for k, v in (headers or {}).items()}
        self.body = body


class UpstreamResponse:
    def __init__(
        self,
        status_code: int = 500,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        upstream_name: str = "",
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.body = body
        self.upstream_name = upstream_name


class UpstreamTransport(Protocol):
    async def send(
        self, base_url: str, request: UpstreamRequest, *, timeout: float
    ) -> UpstreamResponse: ...


class HttpxTransport:
    """Thin httpx wrapper that normalizes network errors.

    Connection failures -> UpstreamConnectionError
    Timeouts            -> UpstreamTimeout
    HTTP statuses are returned normally (a 5xx is still a response; the
    handler decides about retries/circuit accounting).
    """

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient()
        return self._client

    async def send(
        self, base_url: str, request: UpstreamRequest, *, timeout: float
    ) -> UpstreamResponse:
        client = await self._get_client()
        url = base_url.rstrip("/") + "/" + request.path.lstrip("/")
        try:
            resp = await client.request(
                request.method,
                url,
                headers=request.headers,
                content=request.body or None,
                timeout=timeout,
            )
        except httpx.TimeoutException as exc:
            raise UpstreamTimeout(str(exc)) from exc
        except httpx.TransportError as exc:
            raise UpstreamConnectionError(str(exc)) from exc
        return UpstreamResponse(
            status_code=resp.status_code,
            headers=dict(resp.headers),
            body=resp.content,
        )

    async def aclose(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
