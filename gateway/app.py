"""FastAPI application exposing the sidecar gateway."""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from gateway.config import GatewayConfig, default_config
from gateway.errors import FailureReason
from gateway.handler import GatewayHandler
from gateway.logging_utils import configure_logging
from gateway.transport import HttpxTransport, UpstreamRequest, UpstreamTransport

logger = logging.getLogger("gateway.app")

# Distinguishable, stable status mapping for degraded responses.
_REASON_STATUS = {
    FailureReason.NO_ROUTE.value: 404,
    FailureReason.NO_HEALTHY_INSTANCE.value: 503,
    FailureReason.CIRCUIT_OPEN.value: 503,
    FailureReason.UPSTREAM_TIMEOUT.value: 504,
    FailureReason.UPSTREAM_UNREACHABLE.value: 502,
    FailureReason.RETRIES_EXHAUSTED.value: 502,
}

_HOP_BY_HOP = frozenset(
    {
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
)


def build_default_handler(config: GatewayConfig | None = None) -> GatewayHandler:
    config = config or default_config()
    return GatewayHandler(config, HttpxTransport())


def create_app(
    config: GatewayConfig | None = None,
    transport: UpstreamTransport | None = None,
    handler: GatewayHandler | None = None,
) -> FastAPI:
    configure_logging()
    config = config or default_config()
    if handler is None:
        handler = GatewayHandler(config, transport or HttpxTransport())

    app = FastAPI(title="resilient-sidecar-gateway")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def gateway_entry(full_path: str, request: Request) -> Response:
        body = await request.body()
        upstream_request = UpstreamRequest(
            method=request.method,
            path="/" + full_path,
            headers={k: v for k, v in request.headers.items()},
            body=body,
        )
        result = await handler.handle(upstream_request)

        if result.response is not None:
            resp = result.response
            headers = {
                k: v
                for k, v in resp.headers.items()
                if k.lower() not in _HOP_BY_HOP
            }
            headers["x-gateway-served-by"] = resp.upstream_name or ""
            headers["x-gateway-attempts"] = str(result.attempts)
            if result.reason:
                headers["x-gateway-failure"] = result.reason
            return Response(
                content=resp.body,
                status_code=resp.status_code,
                headers=headers,
            )

        assert result.reason is not None
        status_code = _REASON_STATUS.get(result.reason, 502)
        payload = {
            "error": "bad_gateway",
            "reason": result.reason,
            "attempts": result.attempts,
        }
        logger.info("degraded response reason=%s attempts=%d", result.reason, result.attempts)
        return JSONResponse(
            status_code=status_code,
            content=payload,
            headers={
                "x-gateway-failure": result.reason,
                "x-gateway-attempts": str(result.attempts),
            },
        )

    return app
