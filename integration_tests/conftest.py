import logging
import os
import sys
import urllib.request
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from docker_manager import DockerManager

logger = logging.getLogger(__name__)

COMPOSE_FILE = str(Path(__file__).parent / "docker-compose.yml")
LIVY_URL = os.environ.get("LIVY_URL", "http://localhost:8998")


def _resolve_livy_url(base_url: str) -> str:
    """
    Probe for a reachable Livy instance.

    In Docker-in-Docker environments (e.g., devcontainers), ``localhost``
    may not reach the Livy container. This function tries the given
    URL first, then falls back to ``host.docker.internal``.
    """
    candidates = [base_url]

    if "localhost" in base_url or "127.0.0.1" in base_url:
        alt = base_url.replace("localhost", "host.docker.internal").replace("127.0.0.1", "host.docker.internal")
        if alt != base_url:
            candidates.append(alt)

    for url in candidates:
        try:
            urllib.request.urlopen(f"{url}/sessions", timeout=5)
            logger.info("Livy reachable at %s", url)
            return url
        except Exception:
            logger.debug("Livy not reachable at %s", url)

    logger.warning("Livy not reachable at any candidate URL; using %s", base_url)
    return base_url


@pytest.fixture(scope="session")
def docker_spark_livy():
    """
    Session-scoped fixture that starts Spark+Livy and SQL Server via
    Docker Compose and tears them down after all tests.

    Set SPARK_SKIP_DOCKER=1 to skip Docker management (e.g., when
    Spark+Livy is already running externally).
    """
    if os.environ.get("SPARK_SKIP_DOCKER", "0") == "1":
        logger.info("Skipping Docker management (SPARK_SKIP_DOCKER=1)")
        resolved = _resolve_livy_url(LIVY_URL)
        yield resolved
        return

    manager = DockerManager(compose_file=COMPOSE_FILE)

    try:
        logger.info("Starting Docker Compose (Spark+Livy + SQL Server)...")
        manager.nuke()
        manager.up(detach=True, wait=True, timeout=300)
        manager.wait_for_healthy(service="spark-livy", timeout=300)
        resolved = _resolve_livy_url(LIVY_URL)
        logger.info("Livy is ready at %s", resolved)
        yield resolved
    finally:
        logger.info("Stopping Docker Compose...")
        logs = manager.logs(tail=50)
        logger.info("Spark+Livy logs (last 50 lines):\n%s", logs)
        manager.nuke()


@pytest.fixture(scope="session")
def dbt_project_dir():
    """Return the path to the dbt-adventureworks project directory."""
    return str(Path(__file__).parent / "dbt-adventureworks")
