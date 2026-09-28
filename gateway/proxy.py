"""网关核心：选路 -> 健康过滤/负载均衡 -> 熔断判定 -> 转发
-> 健康与熔断记账 -> 带退避的可安全重试。

关键一致性约定：
- 实例被判定 UNHEALTHY 后绝不会再被选中；对应熔断器 OPEN 后也直接跳过；
- 熔断器拒绝（OPEN / 半开名额已满）不消耗重试次数，优先切换其它实例；
- 真正尝试了上游（传输/HTTP 失败）才消耗一次 attempt，并按策略退避；
- 4xx（除 408/429）视为确定性客户端错误，直接返回，不记失败、不重试；
- 传输错误与 5xx 同时计入“被动健康”和“熔断器”；
- 携带幂等键的写请求在整个重试过程中只占位一次，上游不会收到重复提交；
  终态响应（含确定性 4xx）会被缓存并在重放时直接返回；
- 最终失败抛出归一化 GatewayError（reason 可区分）。
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any

from gateway.balancer import RoundRobinBalancer
from gateway.circuit import CircuitRegistry
from gateway.clock import Clock, SystemClock
from gateway.config import GatewayConfig, RouteConfig
from gateway.errors import (
    FailureReason,
    GatewayError,
    UpstreamUnavailable,
    make_upstream_http_error,
)
from gateway.events import EventLog
from gateway.health import HealthTracker
from gateway.idempotency import IdempotencyStore
from gateway.retry import backoff_delay, is_method_retryable
from gateway.transport import Transport, UpstreamRequest, UpstreamResponse

IDEMPOTENCY_HEADER = "idempotency-key"


@dataclass
class AttemptResult:
    response: UpstreamResponse | None = None
    error: GatewayError | None = None
    upstream_id: str = ""

    @property
    def ok(self) -> bool:
        return self.response is not None


class Gateway:
    def __init__(
        self,
        config: GatewayConfig,
        transport: Transport,
        *,
        event_log: EventLog,
        clock: Clock | None = None,
        idem_store: IdempotencyStore | None = None,
        rng: random.Random | None = None,
        sleep=None,
    ) -> None:
        self._cfg = config
        self._transport = transport
        self._log = event_log
        self._clock = clock or SystemClock()
        self._health = HealthTracker(config.upstreams, event_log, self._clock)
        self._circuits = CircuitRegistry(event_log, self._clock)
        self._balancer = RoundRobinBalancer()
        self._idem = idem_store or IdempotencyStore(self._clock)
        self._rng = rng or random.Random()
        self._sleep = sleep or asyncio.sleep

    # ---- 路由匹配 -------------------------------------------------------
    def match_route(self, path: str) -> tuple[RouteConfig, str] | None:
        """按最长前缀匹配，返回 (路由, 转发到上游的路径)。"""
        for route in self._cfg.routes:
            if path == route.prefix or path.startswith(route.prefix.rstrip("/") + "/"):
                upstream_path = path if not route.strip_prefix else path[len(route.prefix):]
                if not upstream_path.startswith("/"):
                    upstream_path = "/" + upstream_path
                return route, upstream_path
        return None

    # ---- 主转发流程 -----------------------------------------------------
    async def forward(
        self,
        *,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> UpstreamResponse:
        matched = self.match_route(path)
        if matched is None:
            raise GatewayError(
                FailureReason.NO_HEALTHY_UPSTREAM,
                f"no route matches {path}",
                status_code=404,
                details={"path": path},
            )
        route, upstream_path = matched

        # 幂等占位：命中 DONE 直接回放；并发同 key 得到 409
        idem_key = headers.get(IDEMPOTENCY_HEADER)
        if idem_key:
            record = self._idem.acquire(idem_key)
            if record is not None:
                resp = UpstreamResponse(
                    status_code=record.status_code,
                    body=record.body,
                    headers=dict(record.headers),
                )
                resp.headers["x-gateway-idempotent-replay"] = ["true"]
                return resp

        retry_cfg = route.retry
        can_retry_method = is_method_retryable(
            method,
            retry_cfg,
            has_idempotency_key=bool(idem_key),
        )
        attempts = 0
        last_error: GatewayError | None = None
        tried: list[str] = []

        try:
            while attempts < retry_cfg.max_attempts:
                # 1) 健康过滤：UNHEALTHY 实例绝不参与选择
                healthy = self._health.healthy_ids(route.upstream_ids)
                candidates = [uid for uid in healthy if uid not in tried]
                if not candidates and healthy:
                    candidates = list(healthy)  # 都试过一轮：允许退避后重试

                # 2) 负载均衡 + 熔断判定：被熔断拒绝的实例不消耗尝试次数
                chosen: str | None = None
                rejected_open: list[str] = []
                while candidates:
                    candidate = self._balancer.choose(route.id, route.upstream_ids, candidates)
                    if candidate is None:
                        break
                    candidates.remove(candidate)
                    breaker = self._circuits.get(route.id, candidate, route.circuit)
                    if breaker.allow_request():
                        chosen = candidate
                        break
                    rejected_open.append(candidate)

                if chosen is None:
                    if healthy:
                        raise GatewayError(
                            FailureReason.CIRCUIT_OPEN,
                            f"circuit open for all upstreams of route {route.id}",
                            status_code=503,
                            details={"route": route.id, "open": rejected_open},
                        )
                    raise GatewayError(
                        FailureReason.NO_HEALTHY_UPSTREAM,
                        f"no healthy upstream for route {route.id}",
                        status_code=503,
                        details={"route": route.id},
                    )

                # 3) 真正向上游发起一次尝试
                attempts += 1
                result = await self._attempt(
                    route, chosen, method, upstream_path, headers, body
                )

                if result.ok:
                    resp = result.response
                    assert resp is not None
                    if idem_key:
                        self._idem.complete(idem_key, resp.status_code, resp.body, resp.headers)
                    return resp

                err = result.error
                assert err is not None
                last_error = err
                if result.upstream_id not in tried:
                    tried.append(result.upstream_id)

                # 4) 非可重试错误（方法不安全或错误确定性）：直接返回/抛出
                if not (can_retry_method and err.retryable):
                    if idem_key:
                        # 确定性终态（如 4xx）可安全缓存与回放
                        if err.__class__.__name__ == "UpstreamHTTPError" and not err.retryable:
                            status = getattr(err, "upstream_status", err.status_code)
                            self._idem.complete(idem_key, status, str(err).encode(), {})
                        else:
                            self._idem.fail(idem_key)
                    raise err

                if attempts >= retry_cfg.max_attempts:
                    break
                await backoff_delay(attempts, retry_cfg, self._sleep, self._rng)

            # 只尝试过一次（max_attempts=1）：保留可区分的归一化上游原因，
            # 不包装成 retries_exhausted，避免吞掉真实失败原因。
            if attempts <= 1 and last_error is not None:
                if idem_key:
                    self._idem.fail(idem_key)
                raise last_error

            reason = last_error.reason if last_error else FailureReason.RETRIES_EXHAUSTED
            if idem_key:
                self._idem.fail(idem_key)
            raise GatewayError(
                FailureReason.RETRIES_EXHAUSTED,
                f"retries exhausted after {attempts} attempt(s); last={reason.value}",
                status_code=502,
                details={
                    "route": route.id,
                    "attempts": attempts,
                    "last_reason": reason.value,
                    "tried": tried,
                },
            )
        except GatewayError:
            raise
        except Exception as exc:  # noqa: BLE001 - pragma: no cover - 防御性兜底归一化
            if idem_key:
                self._idem.fail(idem_key)
            raise GatewayError(FailureReason.INTERNAL, repr(exc)) from exc

    async def _attempt(
        self,
        route: RouteConfig,
        upstream_id: str,
        method: str,
        upstream_path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> AttemptResult:
        breaker = self._circuits.get(route.id, upstream_id, route.circuit)
        base_url = self._cfg.upstreams[upstream_id].base_url
        req = UpstreamRequest(
            method=method.upper(),
            path=upstream_path,
            headers=headers,
            body=body,
            timeout=route.timeout,
        )
        try:
            resp = await self._transport.send(base_url, req)
        except UpstreamUnavailable as err:
            self._health.record_failure(upstream_id)
            breaker.record_failure()
            err.retryable = True
            return AttemptResult(error=err, upstream_id=upstream_id)
        except Exception as err:  # noqa: BLE001 - 自定义传输的未归一化异常兜底归一化
            wrapped = UpstreamUnavailable(
                FailureReason.UPSTREAM_CONNECTION,
                f"{upstream_id}: {err!r}",
                retryable=True,
            )
            self._health.record_failure(upstream_id)
            breaker.record_failure()
            return AttemptResult(error=wrapped, upstream_id=upstream_id)

        if resp.status_code >= 400:
            http_err = make_upstream_http_error(resp.status_code, upstream_id)
            if http_err.retryable:  # 408 / 429 / 5xx
                self._health.record_failure(upstream_id)
                breaker.record_failure()
            else:
                # 确定性 4xx：不记健康失败，但让熔断器在半开探测时视为探测成功
                breaker.record_success()
                self._health.record_success(upstream_id)
            return AttemptResult(error=http_err, upstream_id=upstream_id)

        breaker.record_success()
        self._health.record_success(upstream_id)
        resp.headers["x-gateway-upstream"] = [upstream_id]
        return AttemptResult(response=resp, upstream_id=upstream_id)

    # ---- 主动探测（由后台循环调用） --------------------------------------
    async def run_probes(self) -> None:
        for uid in self._cfg.upstreams:
            if self._health.probe_due(uid):
                await self._health.probe(uid, self._transport)

    # ---- 诊断 -----------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        return {
            "health": self._health.snapshot(),
            "circuits": self._circuits.states(),
            "idempotency": self._idem.snapshot(),
        }

    async def aclose(self) -> None:
        await self._transport.aclose()
