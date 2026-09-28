"""End-to-end tests through the FastAPI app using a scripted httpx transport."""

import httpx
import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config import GatewayConfig, RouteConfig, UpstreamConfig
from gateway.retry import RetryPolicy
from gateway.transport import HttpxTransport


def _config() -> GatewayConfig:
    return GatewayConfig(
        routes=(
            RouteConfig(
                prefix="/api",
                upstreams=(
                    UpstreamConfig(name="a1", base_url="http://a1"),
                    UpstreamConfig(name="a2", base_url="http://a2"),
                ),
                timeout=1.0,
                retry_policy=RetryPolicy(max_attempts=1, base_delay=0),
            ),
        ),
        unhealthy_threshold=2,
        health_cooldown=100.0,
        failure_threshold=100,
        open_cooldown=100.0,
    )


def _handler_with_httpx_mock(script):
    config = _config()

    def handler(request: httpx.Request) -> httpx.Response:
        host = str(request.url).split("/")[2]
        behavior = script.get(host)
        if behavior == "unreachable":
            raise httpx.ConnectError("connection refused")
        if behavior == "timeout":
            raise httpx.ReadTimeout("timed out")
        status, body = behavior
        return httpx.Response(status, content=body, headers={"x-up": host})

    transport = HttpxTransport()
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return create_app(config=config, transport=transport)


def test_success_is_proxied_with_gateway_headers() -> None:
    app = _handler_with_httpx_mock({"a1": (200, b"ok-a1"), "a2": (200, b"ok-a2")})
    client = TestClient(app)
    r = client.get("/api/hello")
    assert r.status_code == 200
    assert r.headers["x-gateway-served-by"] in ("a1", "a2")
    assert r.headers["x-gateway-attempts"] == "1"
    assert "x-gateway-failure" not in r.headers
    # prefix stripped when forwarding: upstream received /hello
    assert r.content in (b"ok-a1", b"ok-a2")


def test_no_route_returns_normalized_404() -> None:
    app = _handler_with_httpx_mock({})
    r = TestClient(app).get("/nope/x")
    assert r.status_code == 404
    body = r.json()
    assert body["reason"] == "no_route"
    assert r.headers["x-gateway-failure"] == "no_route"


def test_all_unreachable_returns_502_with_reason() -> None:
    app = _handler_with_httpx_mock({"a1": "unreachable", "a2": "unreachable"})
    r = TestClient(app).get("/api/x")
    assert r.status_code == 502
    body = r.json()
    assert body["reason"] == "upstream_unreachable"
    assert r.headers["x-gateway-failure"] == "upstream_unreachable"


def test_timeout_is_distinguishable_from_unreachable() -> None:
    app = _handler_with_httpx_mock({"a1": "timeout", "a2": "timeout"})
    r = TestClient(app).get("/api/slow")
    assert r.status_code == 504
    assert r.json()["reason"] == "upstream_timeout"


def test_5xx_response_is_returned_and_annotated() -> None:
    app = _handler_with_httpx_mock({"a1": (500, b"boom"), "a2": (503, b"down")})
    r = TestClient(app).get("/api/fail")
    assert r.status_code in (500, 503)
    assert r.headers["x-gateway-failure"].startswith("upstream_5xx_")


def test_4xx_passes_through_untouched() -> None:
    app = _handler_with_httpx_mock({"a1": (404, b"missing"), "a2": (404, b"missing")})
    r = TestClient(app).get("/api/missing")
    assert r.status_code == 404
    assert r.content == b"missing"
    assert "x-gateway-failure" not in r.headers


def test_health_switch_hides_failed_instance() -> None:
    # a1 always fails; after threshold=2 it is ejected and a2 serves reliably.
    app = _handler_with_httpx_mock({"a1": "unreachable", "a2": (200, b"a2-ok")})
    client = TestClient(app)
    results = [client.get("/api/x") for _ in range(6)]
    # Final requests all succeed and are served by a2.
    assert all(r.status_code == 200 for r in results[-3:])
    assert all(r.headers["x-gateway-served-by"] == "a2" for r in results[-3:])


def test_idempotency_key_dedupicates_at_gateway() -> None:
    seen = {"n": 0}

    def script():
        return {"a1": None, "a2": None}

    config = GatewayConfig(
        routes=(
            RouteConfig(
                prefix="/api",
                upstreams=(UpstreamConfig(name="a1", base_url="http://a1"),),
                retry_policy=RetryPolicy(max_attempts=1),
            ),
        ),
        unhealthy_threshold=100,
        health_cooldown=100.0,
        failure_threshold=100,
        open_cooldown=100.0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        seen["n"] += 1
        return httpx.Response(201, content=b"created")

    transport = HttpxTransport()
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(config=config, transport=transport)
    client = TestClient(app)
    headers = {"Idempotency-Key": "order-42"}
    r1 = client.post("/api/orders", headers=headers, content=b"{}")
    r2 = client.post("/api/orders", headers=headers, content=b"{}")
    assert r1.status_code == 201 and r2.status_code == 201
    assert seen["n"] == 1  # upstream executed exactly once
