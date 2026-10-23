"""Pytest helpers shared by integration tests (local PyFlink and optional Docker)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest


@pytest.fixture
def local_stream_env() -> Iterator[object]:
    """Yield a local PyFlink execution environment with parallelism 1."""
    from pyflink.datastream import StreamExecutionEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    yield env


@pytest.fixture
def require_docker() -> None:
    """Skip the test when Docker is not available (local integration only)."""
    try:
        import docker
        from docker.errors import DockerException

        docker.from_env().ping()
    except DockerException:
        pytest.skip("Docker is required for this test")
    except Exception:
        pytest.skip("Docker is required for this test")
