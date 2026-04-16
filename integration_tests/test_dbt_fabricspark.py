"""
Integration tests for dbt-fabricspark using the adventureworks star schema.

These tests run dbt commands against a Spark+Livy instance started
via Docker Compose. They validate the full lifecycle:

  1. dbt debug   - connection verification
  2. dbt deps    - package installation
  3. dbt seed    - data loading via Spark SQL
  4. dbt run     - model deployment as Delta tables
  5. dbt test    - data integrity via Spark SQL
  6. dbt build   - full lifecycle in one command

Requires Docker to be available. Set SPARK_SKIP_DOCKER=1 if Spark+Livy
is already running externally.
"""

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

logger = logging.getLogger(__name__)
_ADAPTER_ROOT = str(Path(__file__).resolve().parent.parent)


def _find_dbt_executable() -> str:
    """
    Locate the ``dbt`` CLI executable.

    When running inside a virtualenv (e.g., via ``pytest``), ``dbt`` lives in
    the same ``bin/`` directory as the Python interpreter but may not be on the
    system ``PATH``.
    """
    venv_dbt = Path(sys.executable).parent / "dbt"
    if venv_dbt.is_file():
        return str(venv_dbt)

    found = shutil.which("dbt")
    if found:
        return found

    raise FileNotFoundError(
        "Cannot locate the 'dbt' executable. Ensure dbt-core is installed in the active virtualenv."
    )


def _run_dbt(
    project_dir: str,
    args: list,
    timeout: int = 600,
) -> subprocess.CompletedProcess:
    """
    Run a dbt command in the given project directory.

    :param project_dir: Path to the dbt project.
    :param args: dbt command arguments (e.g., ["debug", "--target", "local-local"]).
    :param timeout: Command timeout in seconds.
    :return: CompletedProcess result.
    """
    dbt_bin = _find_dbt_executable()
    cmd = [dbt_bin] + args + ["--profiles-dir", project_dir]
    logger.info("Running: %s (in %s)", " ".join(cmd), project_dir)

    env = {
        **os.environ,
        "DBT_PROFILES_DIR": project_dir,
        "PYTHONPATH": os.pathsep.join(
            filter(None, [_ADAPTER_ROOT, os.environ.get("PYTHONPATH", "")])
        ),
    }

    result = subprocess.run(
        cmd,
        cwd=project_dir,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )

    if result.stdout:
        logger.info("stdout:\n%s", result.stdout[-2000:])
    if result.stderr:
        logger.info("stderr:\n%s", result.stderr[-2000:])

    return result


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDbtFabricSparkIntegration:
    """End-to-end integration tests for dbt-fabricspark with adventureworks.

    Tests run in declaration order and form a single lifecycle.
    Seeds are loaded first, then models are built and data tests are
    executed.  Seeds run in a separate step because dbt's DAG does not
    create implicit edges from source-level tests to seed nodes.
    """

    def test_01_dbt_debug(self, docker_spark_livy, dbt_project_dir):
        """Verify that dbt can connect to Spark via Livy."""
        result = _run_dbt(dbt_project_dir, ["debug", "--target", "local-local"])
        assert result.returncode == 0, f"dbt debug failed:\n{result.stdout}\n{result.stderr}"

    def test_02_dbt_deps(self, docker_spark_livy, dbt_project_dir):
        """Install dbt packages (dbt_utils)."""
        result = _run_dbt(dbt_project_dir, ["deps"])
        assert result.returncode == 0, f"dbt deps failed:\n{result.stdout}\n{result.stderr}"

    def test_03_dbt_build_full_refresh(self, docker_spark_livy, dbt_project_dir):
        """
        Full lifecycle: seed first, then build models + tests.

        Seeds are run separately because ``dbt build`` does not create
        implicit DAG edges from source-level tests to the seed nodes
        that populate those sources.  Running seeds first guarantees the
        tables exist before any relationship tests execute.
        """
        # 1. Load seed data so source tables exist
        seed_result = _run_dbt(
            dbt_project_dir,
            ["seed", "--target", "local-local", "--full-refresh"],
            timeout=1800,
        )
        assert seed_result.returncode == 0, (
            f"dbt seed --full-refresh failed:\n{seed_result.stdout}\n{seed_result.stderr}"
        )

        # 2. Build models and run tests (seeds already in place)
        build_result = _run_dbt(
            dbt_project_dir,
            ["build", "--target", "local-local", "--exclude", "resource_type:seed"],
            timeout=1800,
        )
        assert build_result.returncode == 0, (
            f"dbt build --exclude resource_type:seed failed:\n{build_result.stdout}\n{build_result.stderr}"
        )
