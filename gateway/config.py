"""Gateway configuration: routes, upstreams, load balancing, retry policies."""

from dataclasses import dataclass, field

from gateway.retry import RetryPolicy


@dataclass(frozen=True)
class UpstreamConfig:
    name: str
    base_url: str
    weight: int = 1

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("upstream name must not be empty")
        if self.weight < 1:
            raise ValueError("upstream weight must be >= 1")


@dataclass(frozen=True)
class RouteConfig:
    """A route prefix maps to several upstream instances."""

    prefix: str
    upstreams: tuple[UpstreamConfig, ...]
    strip_prefix: bool = True
    timeout: float = 5.0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

    def __post_init__(self) -> None:
        if not self.prefix.startswith("/"):
            raise ValueError("route prefix must start with '/'")
        if not self.upstreams:
            raise ValueError("route must have at least one upstream")
        names = [u.name for u in self.upstreams]
        if len(names) != len(set(names)):
            raise ValueError("upstream names must be unique within a route")


@dataclass(frozen=True)
class GatewayConfig:
    routes: tuple[RouteConfig, ...] = ()
    # consecutive failures before an instance is ejected
    unhealthy_threshold: int = 3
    # seconds an ejected instance stays out before a probe is allowed
    health_cooldown: float = 2.0
    # consecutive failures before a (route, instance) circuit opens
    failure_threshold: int = 5
    # seconds the circuit stays open before a half-open probe is allowed
    open_cooldown: float = 5.0

    def __post_init__(self) -> None:
        prefixes = [r.prefix for r in self.routes]
        if len(prefixes) != len(set(prefixes)):
            raise ValueError("route prefixes must be unique")

    def match_route(self, path: str) -> RouteConfig | None:
        """Longest-prefix match against configured routes."""
        best: RouteConfig | None = None
        for route in self.routes:
            if path == route.prefix or path.startswith(
                route.prefix.rstrip("/") + "/"
            ):
                if best is None or len(route.prefix) > len(best.prefix):
                    best = route
        return best


def default_config() -> GatewayConfig:
    """Config used by the standalone demo (overridable via env in the future)."""
    return GatewayConfig(
        routes=(
            RouteConfig(
                prefix="/api",
                upstreams=(
                    UpstreamConfig(name="a1", base_url="http://127.0.0.1:9101"),
                    UpstreamConfig(name="a2", base_url="http://127.0.0.1:9102"),
                ),
                timeout=3.0,
            ),
        ),
        unhealthy_threshold=3,
        health_cooldown=2.0,
        failure_threshold=5,
        open_cooldown=5.0,
    )
