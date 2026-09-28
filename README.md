# FastAPI Resilient Sidecar Gateway

一个本地可运行的边车式请求网关：在同一**路由**下的多个上游实例之间做负载均衡，
并提供健康摘除/恢复、熔断降级、带幂等语义的重试。所有异常路径都返回**稳定、
可区分、可复现**的归一化结果。

## 架构

```
client ──► gateway (FastAPI, :8080) ──► route matcher (最长前缀)
                  │
                  ├─ LoadBalancer   健康实例轮询 + 冷却后单实例探测
                  ├─ HealthTracker  摘除 / 探测 / 恢复 (每实例独立锁)
                  ├─ CircuitBreaker closed → open → half_open → closed
                  ├─ RetryPolicy    安全分类 + 指数退避全抖动
                  └─ IdempotencyStore  Idempotency-Key 去重 (含并发合并)
```

模块：

| 文件 | 职责 |
| --- | --- |
| `gateway/config.py` | 路由、上游、阈值、重试策略配置；最长前缀匹配 |
| `gateway/health.py` | `HealthTracker` 健康状态机 + `LoadBalancer` 轮询/探测 |
| `gateway/breaker.py` | `CircuitBreaker` 熔断状态机（并发安全） |
| `gateway/retry.py` | 可重试安全分类、退避抖动 |
| `gateway/idempotency.py` | 幂等键存储、并发请求合并、重放 |
| `gateway/transport.py` | httpx 传输，网络错误归一化 |
| `gateway/handler.py` | 请求编排（选实例 → 熔断判定 → 调用 → 记账 → 重试） |
| `gateway/app.py` | FastAPI 装配与归一化降级响应 |
| `gateway/demo.py` | 一个进程跑网关 + 两个上游的本地演示 |

## 路由与负载均衡规则

- **路由匹配**：请求路径按**最长前缀**命中 `RouteConfig.prefix`；未命中返回
  `404 {"reason":"no_route"}`。
- **路径改写**：命中后默认剥掉路由前缀再转发（`strip_prefix=True`），
  例如 `/api/users` → 上游 `/users`。
- **负载均衡**：在当前 `healthy` 实例间做**轮询（round-robin）**。
- **故障摘除**：某实例连续失败达到 `unhealthy_threshold` 次（默认 3）即被摘除，
  状态 `healthy → unhealthy`；摘除期间请求不会再持续打到它。
- **冷却与探测**：摘除经过 `health_cooldown` 秒（默认 2s，演示 5s）后，网关允许
  **唯一一个**恢复探测（`unhealthy → probing`，原子抢占，并发下也只有一个）：
  - 探测成功 → `probing → healthy`，重新纳入轮询；
  - 探测失败 → 重新摘除并刷新冷却窗口。
- 有健康实例时，非幂等且不可重试的请求不会把唯一的一次尝试浪费在探测上；
  可安全重试的请求则允许承担探测（探测失败会自动重试到健康实例）。

## 熔断与降级

每个 `(路由, 实例)` 一个独立熔断器，状态机：

```
          连续失败 ≥ failure_threshold(默认5)
 closed ───────────────────────────────► open
   ▲                                       │
   │ 探测成功                              │ 冷却 open_cooldown(默认5s, 演示8s)
   │                                       ▼
   └─────────────────────────── half_open (只放 1 个探测)
                                  探测失败 → 立刻回 open 并刷新冷却
```

- **open 期间**：请求不再打到该上游，直接返回**可区分的降级结果**
  `503 {"reason":"circuit_open"}`，并优先选择路由内其它实例。
- **half_open**：冷却到期后**原子地**只放入一个探测；其余并发请求全部快速失败，
  因此并发流量不会把状态机来回抖动、也不会产生误判。
- 5xx 与网络错误（连接失败、超时）都计入熔断连续失败；任何成功响应（含 4xx）
  都会清零计数。

## 重试策略

- **只重试安全的失败**：
  - 方法为幂等方法 `GET/HEAD/OPTIONS/PUT/DELETE`；或
  - 请求带 `Idempotency-Key` 头（写请求也可安全重试）。
  - 不带幂等键的 `POST/PATCH` **绝不重试**，只尝试一次。
- **可重试的失败类型**：连接失败、超时、上游 5xx；**4xx 不重试**。
- **次数上限**：`RetryPolicy.max_attempts`（含首次，默认 3）。
- **退避**：指数退避 + 全抖动
  `delay = random(0, min(max_delay, base_delay * 2**attempt))`。
- **跨实例重试**：一次请求的各次尝试优先打到不同实例，避免对同一故障点反复尝试。

### 归一化失败原因（可区分、稳定）

网关对所有失败返回统一 JSON（或响应头），reason 取值：

| reason | HTTP 状态 | 含义 |
| --- | --- | --- |
| `no_route` | 404 | 没有匹配的路由 |
| `no_healthy_instance` | 503 | 全部实例被摘除/冷却中 |
| `circuit_open` | 503 | 熔断器打开，被降级 |
| `upstream_timeout` | 504 | 上游超时 |
| `upstream_unreachable` | 502 | 连接失败/不可达 |
| `upstream_5xx_<code>` | 原始 5xx | 上游持续 5xx，重试耗尽，原样返回并打标 |
| `retries_exhausted` | 502 | 兜底：重试耗尽 |

响应头：`x-gateway-failure=<reason>`、`x-gateway-served-by=<实例>`、
`x-gateway-attempts=<次数>`。

## 幂等去重

- 带 `Idempotency-Key` 的请求在网关侧**最多执行一次**：
  - 并发的同键请求会**合并**到同一次上游调用并共享结果；
  - 之后的重放直接返回缓存响应，不再次打到上游；
  - 该键同时透传给上游，支持上游侧去重；
  - 传输级失败时释放该键，允许客户端随后重试。

## 本地验证

### 1) 一键演示（网关 + 两个上游，同一进程）

```bash
uv run python -m gateway.demo
# 网关 :8080，上游 a1 :9101、a2 :9102
```

另开终端：

```bash
# 负载均衡 / 正常转发
curl -s http://127.0.0.1:8080/api/hello
curl -s http://127.0.0.1:9101/ctrl/down        # 让 a1 故障
for i in $(seq 1 6); do curl -s http://127.0.0.1:8080/api/hello; echo; done
curl -s http://127.0.0.1:9101/ctrl/up          # 恢复 a1（冷却后自动探测回归）

# 幂等写：两次同键，上游只产生一次副作用，第二次返回重放
curl -s -X POST http://127.0.0.1:8080/api/orders -H 'Idempotency-Key: k1'
curl -s -X POST http://127.0.0.1:8080/api/orders -H 'Idempotency-Key: k1'

# 5xx / 无路由
curl -s -w '\n[%{http_code}]\n' http://127.0.0.1:8080/api/fail
curl -s -w '\n[%{http_code}]\n' http://127.0.0.1:8080/unknown
```

演示日志会实时打印上游健康变化与熔断状态迁移，例如：

```
INFO [gateway.health]  health /api/a1: healthy -> unhealthy (threshold:http_503, failures=3)
INFO [gateway.health]  health /api/a1: unhealthy -> probing (recovery probe admitted)
INFO [gateway.health]  health /api/a1: probing -> healthy (probe_succeeded, failures=0)
INFO [gateway.breaker] breaker /api/a1: closed -> open (threshold:upstream_timeout, failures=5)
INFO [gateway.breaker] breaker /api/a1: open -> half_open (probe admitted)
INFO [gateway.breaker] breaker /api/a1: half_open -> closed (probe_succeeded, failures=0)
```

### 2) 单元 / 集成测试（结论稳定、原因可区分）

```bash
uv run pytest -q
```

覆盖：

- 健康摘除、流量切换、冷却后探测恢复、探测失败重摘除、并发下仅一个探测；
- 熔断达阈值打开、open 期降级、半开探测成功闭合/失败重开、
  **并发触发只打开一次 / 只放一个探测**（无抖动、无误判）；
- 重试安全分类、次数上限、退避抖动、4xx 不重试、5xx 打标、
  幂等键去重（串行重放与并发合并）、失败后释放键；
- 端到端：归一化 reason 与 HTTP 状态、`x-gateway-*` 响应头。

测试日志（pytest 默认开启 INFO）会打印健康变化与熔断迁移记录。

## 配置

编辑 `gateway/config.py` 的 `default_config()` 或用代码构造 `GatewayConfig`：

```python
GatewayConfig(
    routes=(RouteConfig(
        prefix="/api",
        upstreams=(
            UpstreamConfig("a1", "http://10.0.0.1:8080"),
            UpstreamConfig("a2", "http://10.0.0.2:8080"),
        ),
        timeout=3.0,
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.05),
    ),),
    unhealthy_threshold=3, health_cooldown=2.0,
    failure_threshold=5,   open_cooldown=5.0,
)
```

## 说明与边界

- 幂等存储、健康/熔断状态为**进程内内存**实现，适合边车单实例；多副本部署需换
  成共享存储。
- 传输层仅归一化连接错误与超时；HTTP 状态码（含 5xx）照常返回，由编排层记账。
