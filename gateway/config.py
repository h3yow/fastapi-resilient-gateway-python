"""网关配置模型：路由、上游实例与策略参数。

配置以普通 dict（JSON）形式提供，结构示例见 ``config.example.json``。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from gateway.errors import FailureReason, GatewayError


class ConfigError(GatewayError):
    def __init__(self, message: str) -> None:
        super().__init__(FailureReason.BAD_GATEWAY_CONFIG, message, status_code=500)


@dataclass(frozen=True)
class UpstreamConfig:
    id: str
    base_url: str
    probe_path: str = "/health"
    probe_interval: float = 1.0
    failure_threshold: int = 3
    recover_successes: int = 2


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 0.05
    max_delay: float = 1.0
    jitter: float = 0.1
    retry_post: bool = False


@dataclass(frozen=True)
class CircuitConfig:
    failure_threshold: int = 5
    open_cooldown: float = 0.5
    half_open_max_probes: int = 1
    half_open_successes: int = 1


@dataclass(frozen=True)
class RouteConfig:
    id: str
    prefix: str
    upstream_ids: list[str]
    timeout: float = 3.0
    strip_prefix: bool = True
    retry: RetryConfig = field(default_factory=RetryConfig)
    circuit: CircuitConfig = field(default_factory=CircuitConfig)


@dataclass(frozen=True)
class GatewayConfig:
    routes: list[RouteConfig]
    upstreams: dict[str, UpstreamConfig]


def _require(d: dict[str, Any], key: str, ctx: str) -> Any:
    if key not in d:
        raise ConfigError(f"missing '{key}' in {ctx}")
    return d[key]


def load_config(data: dict[str, Any]) -> GatewayConfig:
    """从普通 dict（通常来自 JSON）构造配置并做基本校验。"""
    raw_upstreams = data.get("upstreams")
    if not isinstance(raw_upstreams, list) or not raw_upstreams:
        raise ConfigError("'upstreams' must be a non-empty list")

    upstreams: dict[str, UpstreamConfig] = {}
    for item in raw_upstreams:
        uc = UpstreamConfig(
            id=str(_require(item, "id", "upstream")),
            base_url=str(_require(item, "base_url", "upstream")).rstrip("/"),
            probe_path=str(item.get("probe_path", "/health")),
            probe_interval=float(item.get("probe_interval", 1.0)),
            failure_threshold=int(item.get("failure_threshold", 3)),
            recover_successes=int(item.get("recover_successes", 2)),
        )
        if uc.id in upstreams:
            raise ConfigError(f"duplicate upstream id: {uc.id}")
        upstreams[uc.id] = uc

    raw_routes = data.get("routes")
    if not isinstance(raw_routes, list) or not raw_routes:
        raise ConfigError("'routes' must be a non-empty list")

    routes: list[RouteConfig] = []
    seen_prefixes: set[str] = set()
    for item in raw_routes:
        rid = str(_require(item, "id", "route"))
        prefix = str(_require(item, "prefix", f"route {rid}"))
        if not prefix.startswith("/"):
            raise ConfigError(f"route {rid}: prefix must start with '/'")
        if prefix in seen_prefixes:
            raise ConfigError(f"duplicate route prefix: {prefix}")
        seen_prefixes.add(prefix)

        ids = list(_require(item, "upstream_ids", f"route {rid}"))
        if not ids:
            raise ConfigError(f"route {rid}: upstream_ids must be non-empty")
        for uid in ids:
            if uid not in upstreams:
                raise ConfigError(f"route {rid}: unknown upstream id '{uid}'")
        if len(set(ids)) != len(ids):
            raise ConfigError(f"route {rid}: duplicated upstream ids")

        retry_raw = item.get("retry", {})
        circuit_raw = item.get("circuit", {})
        rc = RetryConfig(
            max_attempts=int(retry_raw.get("max_attempts", 3)),
            base_delay=float(retry_raw.get("base_delay", 0.05)),
            max_delay=float(retry_raw.get("max_delay", 1.0)),
            jitter=float(retry_raw.get("jitter", 0.1)),
            retry_post=bool(retry_raw.get("retry_post", False)),
        )
        cc = CircuitConfig(
            failure_threshold=int(circuit_raw.get("failure_threshold", 5)),
            open_cooldown=float(circuit_raw.get("open_cooldown", 0.5)),
            half_open_max_probes=int(circuit_raw.get("half_open_max_probes", 1)),
            half_open_successes=int(circuit_raw.get("half_open_successes", 1)),
        )
        if rc.max_attempts < 1:
            raise ConfigError(f"route {rid}: retry.max_attempts must be >= 1")
        if cc.failure_threshold < 1 or cc.half_open_max_probes < 1:
            raise ConfigError(f"route {rid}: circuit thresholds must be >= 1")

        routes.append(
            RouteConfig(
                id=rid,
                prefix=prefix,
                upstream_ids=ids,
                timeout=float(item.get("timeout", 3.0)),
                strip_prefix=bool(item.get("strip_prefix", True)),
                retry=rc,
                circuit=cc,
            )
        )

    # 长前缀优先匹配
    routes.sort(key=lambda r: len(r.prefix), reverse=True)
    return GatewayConfig(routes=routes, upstreams=upstreams)
