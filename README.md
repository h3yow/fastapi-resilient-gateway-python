# FastAPI Resilient Sidecar Gateway

一个本地可运行的边车式请求网关：按路由把请求分发到多个上游实例，做负载均衡、
健康摘除/恢复、熔断降级、带幂等语义的重试。所有异常路径对外返回归一化、
可区分的失败原因。

## 架构

```
client ──> gateway(:8080) ──> upstream svc-a / svc-b / ...
             │
             ├─ balancer    路由内健康实例间轮询
             ├─ health      被动失败摘除 + 主动探测恢复
             ├─ circuit     每 (路由, 上游) 一个熔断器
             ├─ retry       仅安全失败重试，指数退避 + 抖动
             └─ idempotency Idempotency-Key 去重与结果回放
```

## 路由与负载均衡

- 配置见 `config.example.json`：`routes[].prefix` 按**最长前缀**匹配；
  `strip_prefix` 控制转发时是否剥掉前缀。
- 每条路由有 `upstream_ids` 列表，网关只在**健康实例**间做轮询
  （`gateway/balancer.py`）。
- 实例健康判定（`gateway/health.py`）：
  - 初始 `healthy`；连续失败 < `failure_threshold` 为 `suspect`（仍承接流量）；
  - 连续失败 ≥ `failure_threshold` → `unhealthy`，**立即摘除，绝不再被选中**；
  - 后台探测任务按 `probe_interval` 探活，连续 `recover_successes` 次成功才恢复
    `healthy` 并重新纳入轮询。

## 熔断与降级

每个 `(route, upstream)` 一个熔断器（`gateway/circuit.py`）：

```
CLOSED ──连续失败≥failure_threshold──> OPEN
OPEN ──冷却 open_cooldown 到期，首个请求拿到探测名额──> HALF_OPEN
HALF_OPEN ──探测成功累计 half_open_successes──> CLOSED
HALF_OPEN ──任意探测失败──> OPEN（冷却重新计时）
```

- `OPEN` 期间该实例的请求**不再打到上游**；若路由下所有实例都熔断，
  直接返回 `503 + circuit_open`（可区分的降级结果）。
- `HALF_OPEN` 最多放行 `half_open_max_probes` 个并发探测，其余按降级处理；
  状态判断与计数全部在锁内完成，并发不会抖动状态机。
- 若健康摘除先触发，对外原因优先报 `no_healthy_upstream`（健康是更粗的摘除层）。

## 重试与幂等

- 只对**可安全重试的失败**重试：连接错误、超时、408、429、5xx（501 除外）。
  确定性 4xx 立即返回、不记账、不重试。
- 方法安全：`GET/HEAD/OPTIONS/PUT/DELETE` 默认可重试；`POST` 仅当
  `retry_post: true` 或请求带 `Idempotency-Key` 时重试。
- 退避：`min(max_delay, base_delay * 2^(n-1))` 再乘 `[1-jitter, 1+jitter]` 抖动。
- 幂等（`gateway/idempotency.py`）：
  - 携带 `Idempotency-Key` 的写请求在网关内占位，整个重试过程上游不会收到
    重复提交（键会透传给上游，上游也应按键去重）；
  - 同键并发请求得到 `409 idempotency_conflict`；
  - 终态响应（成功或确定性 4xx）被缓存，同键重放直接返回首次结果，
    响应头带 `x-gateway-idempotent-replay: true`；
  - 上游最终失败（重试耗尽）释放键，客户端可用同键重新发起。

## 归一化失败原因

所有错误响应都是统一信封，且带 `x-gateway-error` 头：

```json
{"error": {"reason": "...", "message": "...", "details": {...}}}
```

| reason | HTTP | 含义 |
| --- | --- | --- |
| `upstream_connection` | 502 | 上游连接失败 |
| `upstream_timeout` | 504 | 上游超时 |
| `upstream_server` | 透传上游状态码 | 上游 5xx |
| `upstream_http` | 透传上游状态码 | 上游其它 4xx |
| `upstream_unauthorized` | 401/403 | 上游鉴权失败 |
| `upstream_rate_limited` | 429 | 上游限流 |
| `no_healthy_upstream` | 503 | 路由下没有健康实例 |
| `circuit_open` | 503 | 熔断降级（请求未打到上游） |
| `retries_exhausted` | 502 | 重试耗尽，`details.last_reason` 给出最后原因 |
| `idempotency_conflict` | 409 | 同幂等键请求正在处理中 |

## 诊断端点

- `GET /_gateway/healthz` — 网关存活
- `GET /_gateway/state` — 各上游健康状态与各熔断器状态
- `GET /_gateway/events` — 健康变化与熔断迁移的事件记录（测试日志中同样可见）

## 运行与验证

```bash
uv sync
uv run pytest -q            # 单测（日志中可见健康/熔断迁移记录）
./scripts/run_demo.sh       # 一键启动两个演示上游 + 网关
```

详细的手动验证步骤（故障注入、摘除/恢复、熔断、幂等）见
[docs/LOCAL_VALIDATION.md](docs/LOCAL_VALIDATION.md)。
