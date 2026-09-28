"""Local demo: two upstreams + the gateway in one process.

Run with:  python -m gateway.demo

Endpoints (upstream app on ports 9101/9102):
  GET  /hello            -> 200 identifying the instance
  POST /orders           -> 201, honors Idempotency-Key (no duplicate side effects)
  GET  /fail             -> 500
  GET  /ctrl/down        -> make this instance fail all requests
  GET  /ctrl/up          -> restore this instance

Gateway on 8080:
  GET  http://127.0.0.1:8080/api/hello
  POST http://127.0.0.1:8080/api/orders  (header Idempotency-Key: <k>)
"""

import asyncio
import threading

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from gateway.app import create_app
from gateway.config import (
    GatewayConfig,
    RouteConfig,
    UpstreamConfig,
)


def build_demo_config() -> GatewayConfig:
    return GatewayConfig(
        routes=(
            RouteConfig(
                prefix="/api",
                upstreams=(
                    UpstreamConfig(name="a1", base_url="http://127.0.0.1:9101"),
                    UpstreamConfig(name="a2", base_url="http://127.0.0.1:9102"),
                ),
                timeout=2.0,
            ),
        ),
        unhealthy_threshold=3,
        health_cooldown=5.0,
        failure_threshold=5,
        open_cooldown=8.0,
    )


def make_upstream_app(name: str, state: dict) -> FastAPI:
    app = FastAPI(title=f"upstream-{name}")
    seen_keys: dict[str, dict] = {}

    @app.get("/hello")
    async def hello() -> dict:
        if state["down"]:
            return JSONResponse(status_code=503, content={"error": "down", "by": name})
        return {"hello": "from", "instance": name}

    @app.post("/orders")
    async def create_order(request: Request) -> JSONResponse:
        if state["down"]:
            return JSONResponse(status_code=503, content={"error": "down", "by": name})
        key = request.headers.get("idempotency-key")
        if key:
            if key in seen_keys:
                cached = seen_keys[key]
                return JSONResponse(
                    status_code=cached["status"],
                    content={**cached["body"], "replayed": True, "by": name},
                )
            body = await request.body()
            record = {"status": 201, "body": {"created": True, "key": key}}
            seen_keys[key] = record
            return JSONResponse(status_code=201, content={**record["body"], "by": name})
        return JSONResponse(status_code=201, content={"created": True, "by": name})

    @app.get("/fail")
    async def fail() -> JSONResponse:
        return JSONResponse(status_code=500, content={"error": "boom", "by": name})

    @app.get("/ctrl/down")
    async def ctrl_down() -> dict:
        state["down"] = True
        return {"instance": name, "state": "down"}

    @app.get("/ctrl/up")
    async def ctrl_up() -> dict:
        state["down"] = False
        return {"instance": name, "state": "up"}

    return app


def _run_server(app, port: int) -> None:
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


async def serve() -> None:
    states = {"a1": {"down": False}, "a2": {"down": False}}
    servers = [
        threading.Thread(
            target=_run_server, args=(make_upstream_app("a1", states["a1"]), 9101),
            daemon=True,
        ),
        threading.Thread(
            target=_run_server, args=(make_upstream_app("a2", states["a2"]), 9102),
            daemon=True,
        ),
    ]
    for t in servers:
        t.start()
    await asyncio.sleep(0.8)

    config = build_demo_config()
    app = create_app(config=config)
    print("Gateway on http://127.0.0.1:8080  (upstreams on 9101/9102)")
    config_uv = uvicorn.Config(app, host="127.0.0.1", port=8080, log_level="info")
    server = uvicorn.Server(config_uv)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(serve())
