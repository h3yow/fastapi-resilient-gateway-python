#!/usr/bin/env bash
# 一键本地演示：启动两个演示上游 + 网关。
# 手动验证步骤见 docs/LOCAL_VALIDATION.md
set -euo pipefail
cd "$(dirname "$0")/.."

uv run python scripts/demo_upstream.py --name svc-a --port 9101 &
A_PID=$!
uv run python scripts/demo_upstream.py --name svc-b --port 9102 &
B_PID=$!
uv run python main.py config.example.json --port 8080 &
GW_PID=$!

trap 'kill $A_PID $B_PID $GW_PID 2>/dev/null || true' EXIT
wait
