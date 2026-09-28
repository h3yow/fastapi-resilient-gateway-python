"""FastAPI 应用工厂：把 Gateway 挂成边车服务。

对外约定：
- 业务请求按最长前缀路由转发（catch-all）；
- 所有网关错误都有统一 JSON 信封：
  ``{"error": {"reason", "message", "details"}}``，reason 归一化、可区分；
- ``/_gateway/healthz``：网关存活；
- ``/_gateway/state``：查看各上游健康与熔断状态；
- ``/_gateway/events``：查看健康变化与熔断迁移记录；
- 后台周期执行主动健康探测。
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from gateway.errors import GatewayError
from gateway.proxy import Gateway
from gateway.transport import UpstreamResponse

# 回传给客户端时需要剥离的 hop-by-hop / 网关内部头
_DROP_RESPONSE_HEADERS = {
    "connection",
    "keep-alive",
    "transfer-encoding",
    "content-length",
    "content-encoding",
}


def _error_response(exc: GatewayError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "reason": exc.reason.value,
                "message": str(exc),
                "details": exc.details,
            }
        },
        headers={"x-gateway-error": exc.reason.value},
    )


def _upstream_response(resp: UpstreamResponse) -> Response:
    headers = {
        k: ", ".join(v)
        for k, v in resp.headers.items()
        if k.lower() not in _DROP_RESPONSE_HEADERS
    }
    return Response(
        content=resp.body,
        status_code=resp.status_code,
        headers=headers,
        media_type=headers.get("content-type"),
    )


def create_app(gateway: Gateway, *, probe_interval: float = 0.2) -> FastAPI:
    log = logging.getLogger("gateway")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(_probe_loop())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001 - 关闭路径记录但不阻断清理
                log.warning("probe task stopped with error: %r", exc)
            await gateway.aclose()

    async def _probe_loop() -> None:
        while True:
            try:
                await gateway.run_probes()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 探测循环永不因单次异常退出
                log.warning("probe round failed: %r", exc)
            await asyncio.sleep(probe_interval)

    app = FastAPI(title="resilient-sidecar-gateway", lifespan=lifespan)
    app.state.gateway = gateway

    @app.get("/_gateway/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/_gateway/state")
    async def state() -> dict:
        return gateway.snapshot()

    @app.get("/_gateway/events")
    async def events() -> dict:
        return {
            "events": [
                {"time": e.time, "kind": e.kind, "target": e.target, "message": e.message}
                for e in gateway._log.events()
            ]
        }

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
    )
    async def proxy(full_path: str, request: Request) -> Response:
        body = await request.body()
        headers = {k: v for k, v in request.headers.items()}
        try:
            upstream_resp = await gateway.forward(
                method=request.method,
                path="/" + full_path,
                headers=headers,
                body=body or b"",
            )
        except GatewayError as exc:
            return _error_response(exc)
        return _upstream_response(upstream_resp)

    return app
