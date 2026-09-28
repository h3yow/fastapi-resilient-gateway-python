"""Smoke test placeholder."""


import pytest


@pytest.mark.asyncio
async def test_skeleton_imports() -> None:
    from gateway.app import create_app
    from gateway.config import default_config

    assert default_config is not None
    assert create_app is not None
