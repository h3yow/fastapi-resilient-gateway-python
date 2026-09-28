"""Request orchestration: routing, balancing, breaker, retries, idempotency."""

import asyncio
import logging
from dataclasses import dataclass

from gateway.breaker import CircuitBreaker
from gateway.config import GatewayConfig, RouteConfig, UpstreamConfig
from gateway.errors import FailureReason
from gateway.health import HealthTracker, LoadBalancer
from gateway.idempotency import IdempotencyStore
from gateway.retry import (
    compute_backoff,
    is_retriable_status,
    is_safe_to_retry,
)
from gateway.transport import (
    TransportError,
    UpstreamRequest,
    UpstreamResponse,
    UpstreamTransport,
)

logger = logging.getLogger("gateway.handler")

IDEMPOTENCY_HEADER = "idempotency-key"


@dataclass
class GatewayResult:
    response: UpstreamResponse | None
    reason: str | None
    attempts: int
    served_by: str | None = None


@dataclass
class _AttemptOutcome:
    response: UpstreamResponse | None
    reason: str | None        # normalized failure reason when this attempt failed
    retriable: bool


def _rewrite_path(route: RouteConfig, original_path: str) -> str:
    if not route.strip_prefix:
        return original_path
    rest = original_path[len(route.prefix):]
    return rest if rest.startswith("/") else "/" + rest


class GatewayHandler:
    def __init__(
        self,
        config: GatewayConfig,
        transport: UpstreamTransport,
        *,
        load_balancer: LoadBalancer | None = None,
        breaker: CircuitBreaker | None = None,
        idempotency: IdempotencyStore | None = None,
        sleep=None,
        random_func=None,
    ) -> None:
        self._config = config
        self._transport = transport
        if load_balancer is not None:
            self._lb = load_balancer
            self._health = load_balancer._tracker
        else:
            self._health = HealthTracker(
                config.unhealthy_threshold, config.health_cooldown
            )
            self._lb = LoadBalancer(self._health)
        self._breaker = breaker or CircuitBreaker(
            config.failure_threshold, config.open_cooldown
        )
        self._idempotency = idempotency or IdempotencyStore()
        self._sleep = sleep or asyncio.sleep
        self._random_func = random_func

    async def handle(self, request: UpstreamRequest) -> GatewayResult:
        route = self._config.match_route(request.path)
        if route is None:
            logger.info("no route for %s %s", request.method, request.path)
            return GatewayResult(None, FailureReason.NO_ROUTE.value, 0)

        key = request.headers.get(IDEMPOTENCY_HEADER)
        outcome = await self._idempotency.begin(key)
        if outcome.status == "replay":
            resp = outcome.stored
            assert resp is not None
            logger.info("idempotency replay key=%s upstream=%s", key, resp.upstream_name)
            return GatewayResult(resp, None, 0, resp.upstream_name)
        if outcome.status == "in_flight":
            assert outcome.future is not None
            resp = await self._idempotency.wait_in_flight(outcome.future)
            logger.info("idempotency coalesced key=%s upstream=%s", key, resp.upstream_name)
            return GatewayResult(resp, None, 0, resp.upstream_name)

        try:
            result = await self._execute(route, request)
        except BaseException:
            if key is not None:
                await self._idempotency.fail(key)
            raise
        if result.response is not None:
            if key is not None:
                await self._idempotency.complete(key, result.response)
        elif key is not None:
            await self._idempotency.fail(key)
        return result

    async def _execute(self, route: RouteConfig, request: UpstreamRequest) -> GatewayResult:
        policy = route.retry_policy
        safe = is_safe_to_retry(request.method, IDEMPOTENCY_HEADER in request.headers)
        max_attempts = policy.max_attempts if safe else 1
        excluded: set[str] = set()
        last_reason = FailureReason.RETRIES_EXHAUSTED.value
        last_response: UpstreamResponse | None = None
        last_upstream: str | None = None

        for attempt in range(max_attempts):
            # On the final attempt ignore exclusions and take whatever is
            # available, so an open circuit surfaces as its own reason instead
            # of a generic "no healthy instance".
            pick_exclude = (
                frozenset() if attempt == max_attempts - 1 else frozenset(excluded)
            )

            async def _select(*, probe: bool):
                return await self._lb.select(
                    route.prefix,
                    route.upstreams,
                    exclude=pick_exclude,
                    allow_probe=probe,
                )

            if safe:
                # Retriable requests may spend an attempt on a recovery probe.
                selection = await _select(probe=True)
            else:
                # Non-idempotent, non-retryable requests prefer healthy peers;
                # probe only as a last resort when nothing healthy exists.
                selection = await _select(probe=False)
                if selection is None:
                    selection = await _select(probe=True)

            if selection is None and pick_exclude and attempt < max_attempts - 1:
                # Every instance was already tried this round and more attempts
                # remain: retry the selection unfiltered so a breaker surfaces
                # its own distinguishable reason on the final attempt.
                pick_exclude = frozenset()
                selection = await _select(probe=safe)
            if selection is None:
                if excluded:
                    return GatewayResult(
                        None, FailureReason.CIRCUIT_OPEN.value, attempt
                    )
                return GatewayResult(
                    None, FailureReason.NO_HEALTHY_INSTANCE.value, attempt
                )

            upstream: UpstreamConfig = selection.upstream
            decision = await self._breaker.allow(route.prefix, upstream.name)
            if not decision.allowed:
                if selection.is_probe:
                    await self._health.release_probe(route.prefix, upstream.name)
                excluded.add(upstream.name)
                last_reason = FailureReason.CIRCUIT_OPEN.value
                logger.info(
                    "breaker %s for %s/%s, selecting another instance",
                    decision.state.value,
                    route.prefix,
                    upstream.name,
                )
                continue

            last_upstream = upstream.name
            outcome = await self._attempt(route, request, upstream)

            if outcome.response is not None and outcome.reason is None:
                return GatewayResult(outcome.response, None, attempt + 1, upstream.name)

            if outcome.response is not None:
                last_response = outcome.response
            last_reason = outcome.reason or last_reason
            excluded.add(upstream.name)

            if outcome.retriable and attempt < max_attempts - 1:
                delay = compute_backoff(policy, attempt, random_func=self._random_func)
                logger.info(
                    "retry %s/%s attempt %d/%d failed=%s backoff=%.4fs",
                    route.prefix,
                    upstream.name,
                    attempt + 1,
                    max_attempts,
                    last_reason,
                    delay,
                )
                await self._sleep(delay)
                continue
            break

        if last_response is not None:
            # Retries exhausted but upstream kept answering (5xx): return its
            # body annotated with the normalized, distinguishable reason.
            last_response.headers["x-gateway-failure"] = last_reason
            return GatewayResult(last_response, last_reason, max_attempts, last_upstream)
        return GatewayResult(None, last_reason, max_attempts, last_upstream)

    async def _attempt(
        self,
        route: RouteConfig,
        request: UpstreamRequest,
        upstream: UpstreamConfig,
    ) -> _AttemptOutcome:
        upstream_path = _rewrite_path(route, request.path)
        try:
            response = await self._transport.send(
                upstream.base_url,
                UpstreamRequest(
                    method=request.method,
                    path=upstream_path,
                    headers=dict(request.headers),
                    body=request.body,
                ),
                timeout=route.timeout,
            )
        except TransportError as exc:
            await self._health.record_failure(
                route.prefix, upstream.name, reason=exc.reason.value
            )
            await self._breaker.record_failure(
                route.prefix, upstream.name, reason=exc.reason.value
            )
            return _AttemptOutcome(None, exc.reason.value, retriable=True)
        except Exception as exc:  # defensive: never surface raw unknown errors
            logger.exception("unexpected transport error: %s", exc)
            await self._health.record_failure(
                route.prefix, upstream.name, reason="unexpected"
            )
            await self._breaker.record_failure(
                route.prefix, upstream.name, reason="unexpected"
            )
            return _AttemptOutcome(
                None, FailureReason.UPSTREAM_UNREACHABLE.value, retriable=True
            )

        response.upstream_name = upstream.name

        if response.status_code < 500:
            # A served response (2xx/3xx/4xx) proves health; 4xx is a client
            # error, never retried.
            await self._health.record_success(route.prefix, upstream.name)
            await self._breaker.record_success(route.prefix, upstream.name)
            return _AttemptOutcome(response, None, retriable=False)

        # 5xx: reachable but failing — counts toward ejection and breaking.
        reason = f"{FailureReason.UPSTREAM_5XX.value}_{response.status_code}"
        await self._health.record_failure(
            route.prefix, upstream.name, reason=f"http_{response.status_code}"
        )
        await self._breaker.record_failure(
            route.prefix, upstream.name, reason=f"http_{response.status_code}"
        )
        return _AttemptOutcome(
            response, reason, retriable=is_retriable_status(response.status_code)
        )
