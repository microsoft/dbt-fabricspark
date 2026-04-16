import logging
import subprocess
import time
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


class DockerManager:
    """
    Manages Docker Compose lifecycle for integration tests.

    Wraps the ``docker compose`` CLI to start, stop, and health-check
    containers for the Spark+Livy integration test stack.
    """

    def __init__(
        self,
        compose_file: str,
        project_name: str = "fabricspark-dbt-test",
    ) -> None:
        self._compose_file = str(Path(compose_file).resolve())
        self._project_name = project_name

    def _run(self, args: List[str], check: bool = True, timeout: Optional[int] = None) -> subprocess.CompletedProcess:
        cmd = [
            "docker",
            "compose",
            "-f",
            self._compose_file,
            "-p",
            self._project_name,
        ] + args

        logger.info("Running: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.stdout:
            logger.debug("stdout: %s", result.stdout[:500])
        if result.stderr:
            logger.debug("stderr: %s", result.stderr[:500])

        if check and result.returncode != 0:
            raise RuntimeError(
                f"Docker compose command failed (exit {result.returncode}):\n"
                f"cmd: {' '.join(cmd)}\n"
                f"stderr: {result.stderr[:1000]}"
            )

        return result

    def up(self, detach: bool = True, wait: bool = True, timeout: int = 300) -> None:
        args = ["up"]
        if detach:
            args.append("-d")
        if wait:
            args.append("--wait")
            args.extend(["--wait-timeout", str(timeout)])

        self._run(args, timeout=timeout + 30)
        logger.info("Docker compose up completed")

    def down(self, volumes: bool = True, timeout: int = 60) -> None:
        args = ["down"]
        if volumes:
            args.append("-v")
        args.append("--remove-orphans")

        self._run(args, check=False, timeout=timeout)
        logger.info("Docker compose down completed")

    def is_healthy(self, service: str = "spark-livy") -> bool:
        result = self._run(
            ["ps", "--format", "json", service],
            check=False,
        )
        return "healthy" in result.stdout.lower()

    def wait_for_healthy(
        self,
        service: str = "spark-livy",
        timeout: int = 300,
        interval: int = 5,
    ) -> None:
        start = time.time()
        while time.time() - start < timeout:
            if self.is_healthy(service):
                logger.info("Service '%s' is healthy", service)
                return
            logger.debug("Waiting for '%s' to become healthy...", service)
            time.sleep(interval)
        raise TimeoutError(f"Service '{service}' did not become healthy within {timeout}s")

    def logs(self, service: Optional[str] = None, tail: int = 100) -> str:
        args = ["logs", "--tail", str(tail)]
        if service:
            args.append(service)
        result = self._run(args, check=False)
        return result.stdout + result.stderr

    def nuke(self) -> None:
        """Force-remove all containers, volumes, and networks for this project."""
        self.down(volumes=True)

        result = subprocess.run(
            ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={self._project_name}"],
            capture_output=True, text=True
        )
        stale = result.stdout.strip()
        if stale:
            for cid in stale.splitlines():
                subprocess.run(["docker", "rm", "-f", cid], capture_output=True)

        subprocess.run(["docker", "network", "rm", "spark-network"], capture_output=True)

        result = subprocess.run(
            ["docker", "volume", "ls", "-q", "--filter", f"label=com.docker.compose.project={self._project_name}"],
            capture_output=True, text=True
        )
        vols = result.stdout.strip()
        if vols:
            for vol in vols.splitlines():
                subprocess.run(["docker", "volume", "rm", "-f", vol], capture_output=True)
