"""Normalized failure reasons shared across the gateway."""

from enum import Enum


class FailureReason(str, Enum):
    """Stable, distinguishable failure reason codes exposed to clients."""

    NO_ROUTE = "no_route"
    NO_HEALTHY_INSTANCE = "no_healthy_instance"
    CIRCUIT_OPEN = "circuit_open"
    UPSTREAM_UNREACHABLE = "upstream_unreachable"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_5XX = "upstream_5xx"
    RETRIES_EXHAUSTED = "retries_exhausted"
    UPSTREAM_4XX = "upstream_4xx"


class GatewayError(Exception):
    """Base error carrying a normalized :class:`FailureReason`."""

    reason: FailureReason = FailureReason.RETRIES_EXHAUSTED

    def __init__(self, reason: FailureReason | None = None, message: str = "") -> None:
        self.reason = reason or self.reason
        super().__init__(message or self.reason.value)
