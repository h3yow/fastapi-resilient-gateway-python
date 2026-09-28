"""演示用上游服务：支持健康检查、回显与故障注入。

用法：
    uv run python scripts/demo_upstream.py --name svc-a --port 9101

接口：
    GET  /health        健康探测
    GET  /echo          回显实例名（演示负载均衡/摘除）
    POST /orders        写接口，按 Idempotency-Key 去重（演示幂等）
    POST /control/fail  {"down": true|false} 切换故障状态（连接不断，返回 503）
"""

from __future__ import annotations

import argparse
import threading

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()
_state = {"down": False}
_lock = threading.Lock()
_seen_keys: set[str] = set()


@app.get("/health")
async def health():
    if _state["down"]:
        return JSONResponse(status_code=503, content={"status": "down"})
    return {"status": "up"}


@app.get("/echo")
async def echo(request: Request):
    if _state["down"]:
        return JSONResponse(status_code=503, content={"error": "forced failure"})
    return {"instance": app.state.name, "path": str(request.url.path)}


@app.post("/orders")
async def create_order(request: Request):
    if _state["down"]:
        return JSONResponse(status_code=503, content={"error": "forced failure"})
    key = request.headers.get("idempotency-key")
    with _lock:
        if key and key in _seen_keys:
            return JSONResponse(
                status_code=200,
                content={"duplicated": True, "instance": app.state.name},
            )
        if key:
            _seen_keys.add(key)
    return JSONResponse(status_code=201, content={"created": True, "instance": app.state.name})


@app.post("/control/fail")
async def control(request: Request):
    body = await request.json()
    _state["down"] = bool(body.get("down"))
    return {"down": _state["down"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    app.state.name = args.name
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
