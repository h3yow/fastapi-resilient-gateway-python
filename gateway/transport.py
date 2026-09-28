"""上游传输层抽象与基于 httpx 的默认实现。

把底层网络差异归一化：连接类异常 -> UPSTREAM_CONNECTION，
超时类异常 -> UPSTREAM_TIMEOUT，统一抛 UpstreamUnavailable，
调用方不需要识别具体库的异常类型。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx

from gateway.errors import FailureReason, UpstreamUnavailable


@dataclass
class UpstreamRequest:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes
    timeout: float


@dataclass
class UpstreamResponse:
    status_code: int
    body: bytes = b""
    headers: dict[str, list[str]] = field(default_factory=dict)


class Transport:
    async def send(self, base_url: str, request: UpstreamRequest) -> UpstreamResponse:
        raise NotImplementedError

    async def health_check(self, base_url: str, path: str, timeout: float = 1.0) -> bool:
        raise NotImplementedError

    async def aclose(self) -> None:
        raise NotImplementedError


# 不允许透传到上游的逐跳/连接管理头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


def filter_request_headers(headers: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP}


class HttpxTransport(Transport):
    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=None)
        return self._client

    async def send(self, base_url: str, request: UpstreamRequest) -> UpstreamResponse:
        client = self._ensure_client()
        url = base_url.rstrip("/") + request.path
        try:
            resp = await client.request(
                request.method,
                url,
                content=request.body or None,
                headers=filter_request_headers(request.headers),
                timeout=request.timeout,
                follow_redirects=False,
            )
        except Exception as exc:
            raise UpstreamUnavailable(
                transport_error_to_reason(exc),
                f"{request.method} {url} failed: {exc!r}",
            ) from exc
        multi: dict[str, list[str]] = {}
        for key, value in resp.headers.multi_items():
            multi.setdefault(key, []).append(value)
        return UpstreamResponse(status_code=resp.status_code, body=resp.content, headers=multi)

    async def health_check(self, base_url: str, path: str, timeout: float = 1.0) -> bool:
        client = self._ensure_client()
        url = base_url.rstrip("/") + path
        try:
            resp = await client.get(url, timeout=timeout)
        except Exception:  # noqa: BLE001 - 健康探测必须把任何异常表达为 False
            return False
        return 200 <= resp.status_code < 400

    async def aclose(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()


def transport_error_to_reason(exc: Exception) -> FailureReason:
    """把传输层异常归一化为 FailureReason。

    TimeoutException（含连接/读/池超时）-> UPSTREAM_TIMEOUT；
    ConnectError 等其它传输错误 -> UPSTREAM_CONNECTION。
    """
    if isinstance(exc, httpx.TimeoutException):
        return FailureReason.UPSTREAM_TIMEOUT
    return FailureReason.UPSTREAM_CONNECTION
