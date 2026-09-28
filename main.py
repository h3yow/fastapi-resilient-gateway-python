"""本地运行入口：python main.py [config.json] [--port N]"""

from __future__ import annotations

import argparse
import json

import uvicorn

from gateway import create_app, load_config
from gateway.clock import SystemClock
from gateway.events import EventLog
from gateway.proxy import Gateway
from gateway.transport import HttpxTransport

DEFAULT_CONFIG_PATH = "config.example.json"


def build_gateway(config_path: str) -> tuple[Gateway, object]:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = load_config(json.load(f))
    event_log = EventLog()
    transport = HttpxTransport()
    gateway = Gateway(cfg, transport, event_log=event_log, clock=SystemClock())
    app = create_app(gateway)
    return gateway, app


def main() -> None:
    parser = argparse.ArgumentParser(description="Resilient sidecar gateway")
    parser.add_argument("config", nargs="?", default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    _, app = build_gateway(args.config)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
