# 本地验证指南

## 0. 准备

```bash
uv sync
```

## 1. 单元测试（自动验证）

```bash
uv run pytest -q
```

测试日志中可直接看到健康变化与熔断迁移记录，例如：

```
[INFO] gateway: event=health_changed target=a | healthy -> suspect (1 consecutive failure(s), threshold=3)
[INFO] gateway: event=health_changed target=a | suspect -> unhealthy (3 consecutive failure(s), threshold=3)
[INFO] gateway: event=circuit_state target=r:a | closed -> open (3 consecutive failures, threshold=3)
[INFO] gateway: event=health_probe target=a | probe ok (2/2)
[INFO] gateway: event=health_changed target=a | unhealthy -> healthy (probe recovery)
```

覆盖的关键场景：

- `tests/test_health.py`：摘除后绝不打流量、探测恢复后重新纳入、全挂归一化原因；
- `tests/test_circuit.py`：开/合/半开迁移、冷却重置、并发计数不丢、迁移恰好一次、
  并发只放行一个半开探测；
- `tests/test_gateway_resilience.py`：熔断降级可区分、跨实例切换、重试上限、
  4xx 不重试、429 重试、POST 默认不重试、并发熔断结论一致；
- `tests/test_idempotency.py`：重试不重复提交、完成结果回放、同键并发 409、
  失败释放键；
- `tests/test_app_http.py`：HTTP 层统一错误信封与诊断端点。

## 2. 本地端到端演示

```bash
./scripts/run_demo.sh
```

会启动：

- 演示上游 `svc-a`（:9101）、`svc-b`（:9102），支持故障注入与幂等键去重；
- 网关（:8080），配置为 `config.example.json`。

### 2.1 负载均衡

```bash
curl -s localhost:8080/orders/echo
curl -s localhost:8080/orders/echo
# 期望交替返回 {"instance":"svc-a",...} / {"instance":"svc-b",...}
```

### 2.2 健康摘除与恢复

```bash
# 让 svc-a 故障（业务与健康探测都返回 503）
curl -s -X POST localhost:9101/control/fail -H content-type:application/json -d '{"down":true}'

# 连续请求：全部落到 svc-b，绝不出现 svc-a
for i in $(seq 8); do curl -s localhost:8080/orders/echo; echo; done

# 查看状态：svc-a 应为 unhealthy
curl -s localhost:8080/_gateway/state | python3 -m json.tool

# 恢复 svc-a，等待探测（probe_interval=1s，需连续 2 次成功）
curl -s -X POST localhost:9101/control/fail -H content-type:application/json -d '{"down":false}'
sleep 4
# 恢复后流量重新在两个实例间轮询
for i in $(seq 8); do curl -s localhost:8080/orders/echo; echo; done
```

### 2.3 熔断降级

把 `config.example.json` 中某路由的 `circuit.failure_threshold` 调小（如 2）、
`open_cooldown` 调大（如 30），并让该路由所有上游故障：

```bash
for i in $(seq 6); do curl -s localhost:8080/orders/echo; echo; done
# 前几次是 upstream_server / upstream_connection；
# 熔断后变为 503 + {"error":{"reason":"circuit_open",...}}，且不再打到上游
```

### 2.4 幂等写

```bash
curl -s -D - -X POST localhost:8080/orders/orders -H 'idempotency-key: order-1' -d '{}'
curl -s -D - -X POST localhost:8080/orders/orders -H 'idempotency-key: order-1' -d '{}'
# 第二次响应带 x-gateway-idempotent-replay: true，上游只执行一次
```

### 2.5 事件记录

```bash
curl -s localhost:8080/_gateway/events | python3 -m json.tool
# 查看 health_changed / health_probe / circuit_state 事件
```
