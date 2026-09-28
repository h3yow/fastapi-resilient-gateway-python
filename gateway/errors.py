"""归一化的对外错误原因与网关异常类型。

设计目标：
- 任意异常路径对外都有一个归一化 ``reason``，且不同失败互不混淆；
- 传输层错误（连接/超时）与上游业务错误（4xx/5xx）分开；
- 是否可安全重试是错误的显式属性，而不是靠调用方猜测。
"""

from __future__ import annotations

import enum
from typing import Any


class FailureReason(str, enum.Enum):
    """对外暴露的、归一化且可互相区分的失败原因。"""

    UPSTREAM_CONNECTION = "upstream_connection"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_SERVER = "upstream_server"
    UPSTREAM_HTTP = "upstream_http"
    UPSTREAM_UNAUTHORIZED = "upstream_unauthorized"
    UPSTREAM_RATE_LIMITED = "upstream_rate_limited"
    NO_HEALTHY_UPSTREAM = "no_healthy_upstream"
    CIRCUIT_OPEN = "circuit_open"
    RETRIES_EXHAUSTED = "retries_exhausted"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    BAD_GATEWAY_CONFIG = "bad_gateway_config"
    INTERNAL = "internal"


# 传输类错误默认使用的对外状态码
_REASON_STATUS: dict[FailureReason, int] = {
    FailureReason.UPSTREAM_CONNECTION: 502,
    FailureReason.UPSTREAM_TIMEOUT: 504,
    FailureReason.UPSTREAM_SERVER: 502,
    FailureReason.UPSTREAM_HTTP: 502,
    FailureReason.UPSTREAM_UNAUTHORIZED: 401,
    FailureReason.UPSTREAM_RATE_LIMITED: 429,
    FailureReason.NO_HEALTHY_UPSTREAM: 503,
    FailureReason.CIRCUIT_OPEN: 503,
    FailureReason.RETRIES_EXHAUSTED: 502,
    FailureReason.IDEMPOTENCY_CONFLICT: 409,
    FailureReason.BAD_GATEWAY_CONFIG: 500,
    FailureReason.INTERNAL: 500,
}


class GatewayError(Exception):
    """网关内部错误基类，携带归一化原因。"""

    reason: FailureReason = FailureReason.INTERNAL
    status_code: int = 500
    retryable: bool = False

    def __init__(
        self,
        reason: FailureReason,
        message: str = "",
        *,
        status_code: int | None = None,
        retryable: bool | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message or reason.value)
        self.reason = reason
        self.status_code = status_code if status_code is not None else _REASON_STATUS[reason]
        if retryable is not None:
            self.retryable = retryable
        self.details = details or {}


class UpstreamUnavailable(GatewayError):
    """上游不可达（连接失败/超时等传输层问题）。"""


class UpstreamHTTPError(GatewayError):
    """上游返回了错误状态码。

    upstream_status 为上游原始状态码；对外默认透传该状态码。
    """

    def __init__(
        self,
        reason: FailureReason,
        message: str,
        *,
        upstream_status: int,
        retryable: bool = False,
    ) -> None:
        super().__init__(
            reason,
            message,
            status_code=upstream_status,
            retryable=retryable,
        )
        self.upstream_status = upstream_status


def classify_http_status(status_code: int) -> tuple[FailureReason, bool]:
    """把上游 HTTP 状态码映射为归一化原因，并指明是否可安全重试。

    返回 ``(原因, 是否可重试)``：
    - 408 / 429 视为可重试的临时失败；
    - 500/502/503/504 视为可重试的服务端失败；
    - 501（Not Implemented）不重试；
    - 其余 4xx 不重试，其余 5xx 可重试。
    """
    if status_code == 401 or status_code == 403:
        return FailureReason.UPSTREAM_UNAUTHORIZED, False
    if status_code == 408:
        return FailureReason.UPSTREAM_TIMEOUT, True
    if status_code == 429:
        return FailureReason.UPSTREAM_RATE_LIMITED, True
    if 400 <= status_code < 500:
        return FailureReason.UPSTREAM_HTTP, False
    if status_code == 501:
        return FailureReason.UPSTREAM_SERVER, False
    if 500 <= status_code < 600:
        return FailureReason.UPSTREAM_SERVER, True
    return FailureReason.UPSTREAM_HTTP, False


def make_upstream_http_error(status_code: int, upstream_id: str) -> UpstreamHTTPError:
    """按上游状态码构造归一化 HTTP 错误。"""
    reason, retryable = classify_http_status(status_code)
    return UpstreamHTTPError(
        reason,
        f"upstream {upstream_id} returned {status_code}",
        upstream_status=status_code,
        retryable=retryable,
    )
