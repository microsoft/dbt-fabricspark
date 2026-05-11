from __future__ import annotations

import copy
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import pytest

from tests.functional.no_schema_groups import group_token_for

logger = logging.getLogger("functional_conftest")

_fail_fast_sentinel: Optional[Path] = None


def _write_fail_sentinel(nodeid: str) -> None:
    """Write the fail-fast sentinel file (exclusive create, first writer wins)."""
    if _fail_fast_sentinel is None or _fail_fast_sentinel.exists():
        return
    try:
        _fail_fast_sentinel.parent.mkdir(parents=True, exist_ok=True)
        with open(_fail_fast_sentinel, "x") as f:
            json.dump(
                {
                    "test": nodeid,
                    "worker": os.environ.get("PYTEST_XDIST_WORKER", "controller"),
                    "timestamp": time.time(),
                },
                f,
            )
    except FileExistsError:
        pass


def pytest_addoption(parser):
    parser.addoption(
        "--schema-mode",
        action="store",
        default="no_schema",
        choices=("no_schema", "with_schema"),
        help="Lakehouse schema mode: no_schema or with_schema",
    )
    parser.addoption(
        "--session-id-file",
        action="store",
        default=None,
        help="Path to shared Livy session ID file for session reuse across xdist workers",
    )
    parser.addoption(
        "--session-id-files",
        action="store",
        default=None,
        help=(
            "Path to a file listing multiple Livy session-ID files (one per line). "
            "xdist workers shard across the listed sessions by worker index so "
            "server-side Spark work is parallelised beyond one session's capacity."
        ),
    )
    parser.addoption(
        "--fail-fast-sentinel",
        action="store",
        default=None,
        help="Path to shared fail-fast sentinel file for cross-session abort on first failure",
    )


def pytest_configure(config):
    global _fail_fast_sentinel
    sentinel = config.getoption("--fail-fast-sentinel", default=None)
    if sentinel:
        _fail_fast_sentinel = Path(sentinel).resolve()


def _expected_cached_session_ids(config) -> set[str]:
    """Read the warmed/cached Livy session IDs the orchestrator wrote.

    The dbt-fabricspark adapter must reuse these session IDs (one per shard)
    rather than starting fresh sessions. We collect the IDs by reading the
    files referenced by ``--session-id-file`` / ``--session-id-files`` so the
    post-test assertion can compare them against the active sessions on the
    lakehouse via ``GET /sessions``.
    """
    expected: set[str] = set()
    sif = config.getoption("--session-id-file", default=None)
    sifs = config.getoption("--session-id-files", default=None)

    paths: list[str] = []
    if sifs:
        try:
            with open(sifs) as f:
                paths.extend(ln.strip() for ln in f if ln.strip())
        except OSError:
            pass
    if sif:
        paths.append(sif)

    for p in paths:
        try:
            sid = Path(p).read_text().strip()
        except OSError:
            continue
        if sid:
            expected.add(sid)
    return expected


@pytest.fixture(scope="session", autouse=True)
def _assert_only_cached_livy_sessions(request):
    """Assert the adapter reuses the cached Livy session(s) per Lakehouse.

    For a given Lakehouse the adapter must ALWAYS reuse the cached session(s)
    written to ``livy-session-id.txt`` by the orchestrator. Any extra Livy
    session observed on the lakehouse after the test run indicates the adapter
    started a session of its own instead of attaching to the warmed one.

    After the test session completes, list the lakehouse's active Livy
    sessions via the Fabric REST API and assert the active set is a subset of
    the warmed/cached IDs. Skipped when no cached IDs are available (ad-hoc
    local runs without the orchestrator).
    """
    yield

    expected_ids = _expected_cached_session_ids(request.config)
    if not expected_ids:
        logger.info(
            "Skipping single-session assertion: no cached Livy session IDs "
            "(test was not driven by the orchestrator)."
        )
        return

    lakehouse_id = os.environ.get("LAKEHOUSE_ID")
    workspace_id = os.environ.get("WORKSPACE_ID_1")
    api_endpoint = os.environ.get("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1")
    if not lakehouse_id or not workspace_id:
        logger.warning("Skipping single-session assertion: LAKEHOUSE_ID / WORKSPACE_ID_1 not set.")
        return

    # Build the Fabric REST client lazily so unit-test imports of this module
    # don't pull in azure-identity unnecessarily.
    from tests.functional.fabric_client import (
        AzureCliTokenProvider,
        FabricClient,
        StaticTokenProvider,
        TokenProvider,
    )

    token_str = os.environ.get("FABRIC_INTEGRATION_TESTS_TOKEN")
    token_provider: TokenProvider = (
        StaticTokenProvider(token_str) if token_str else AzureCliTokenProvider()
    )
    client = FabricClient(workspace_id, api_endpoint, token_provider)

    try:
        active_sessions = client.list_livy_sessions(lakehouse_id)
    except Exception as exc:  # noqa: BLE001 — best-effort post-test guard
        logger.warning(
            "Single-session assertion skipped: failed to list Livy sessions on lakehouse %s: %s",
            lakehouse_id,
            exc,
        )
        return

    active_ids = {str(s["id"]) for s in active_sessions if "id" in s}
    extras = active_ids - expected_ids
    assert not extras, (
        f"dbt-fabricspark started extra Livy sessions on lakehouse {lakehouse_id}: "
        f"unexpected={sorted(extras)} expected={sorted(expected_ids)} "
        f"all_active={sorted(active_ids)}. The adapter must reuse the cached "
        f"session(s) from livy-session-id.txt."
    )
    logger.info(
        "Single-session assertion passed: active=%s expected=%s",
        sorted(active_ids),
        sorted(expected_ids),
    )


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    """Abort test session if a prior test has failed (cross-worker, cross-session).

    Uses ``pytest.skip`` rather than ``pytest.exit`` so the worker cleanly
    skips remaining items. Calling ``pytest.exit`` mid-test trips xdist's
    ``assert not crashitem`` in ``dsession.worker_workerfinished`` (xdist>=3).
    Skipping is cooperative with xdist and still stops the suite effectively
    because every subsequent item short-circuits at setup.
    """
    if _fail_fast_sentinel is not None and _fail_fast_sentinel.exists():
        try:
            info = _fail_fast_sentinel.read_text()
        except OSError:
            info = "(unable to read sentinel)"
        pytest.skip(f"fail-fast: aborting — a prior test failed.\n{info}")


def pytest_runtest_logreport(report):
    """Write fail-fast sentinel on first test failure or setup error."""
    if report.failed and report.when != "teardown":
        _write_fail_sentinel(report.nodeid)


def pytest_collectreport(report):
    """Write fail-fast sentinel on collection errors (e.g. import failures)."""
    if report.failed:
        _write_fail_sentinel(report.nodeid)


def _require_env(key: str) -> str:
    """Return ``os.environ[key]`` or raise a clear ``RuntimeError``.

    Mirrors the orchestrator's helper so a missing env var fails the test
    session at fixture-resolution time rather than letting xdist workers
    surface a confusing ``NoneType`` traceback later.
    """
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(
            f"{key} must be set in test.env or the environment for the functional test suite."
        )
    return value


@pytest.fixture(scope="session")
def workspace_id():
    """Primary Fabric workspace UUID. Raises if ``WORKSPACE_ID_1`` is unset."""
    return _require_env("WORKSPACE_ID_1")


@pytest.fixture(scope="session")
def workspace_name():
    """Display name of the primary Fabric workspace.

    Required by the cross-workspace functional tests for 4-part rendering.
    Raises if ``WORKSPACE_NAME_1`` is unset.
    """
    return _require_env("WORKSPACE_NAME_1")


@pytest.fixture(scope="session")
def ws2_workspace_id():
    """Secondary (read-source) Fabric workspace UUID for cross-workspace tests."""
    return _require_env("WORKSPACE_ID_2")


@pytest.fixture(scope="session")
def ws2_workspace_name():
    """Display name of the secondary Fabric workspace."""
    return _require_env("WORKSPACE_NAME_2")


@pytest.fixture(scope="session")
def ws2_lakehouse_name():
    """Lakehouse name in WS2.

    Populated by the orchestrator's ``provision --workspace ws2`` step. Raises
    if it isn't set, which means the pipeline was invoked out of order.
    """
    return _require_env("WS2_LAKEHOUSE_NAME")


@pytest.fixture(scope="session")
def ws2_lakehouse_id():
    """Lakehouse UUID in WS2.

    Populated by the orchestrator's ``provision --workspace ws2`` step. Raises
    if it isn't set.
    """
    return _require_env("WS2_LAKEHOUSE_ID")


@pytest.fixture(scope="session")
def api_endpoint():
    return os.getenv("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1")


@pytest.fixture(scope="session")
def schema_mode(request):
    return request.config.getoption("--schema-mode")


@pytest.fixture(scope="session")
def is_schema_enabled(schema_mode):
    """Whether the current lakehouse has schemas enabled."""
    return schema_mode == "with_schema"


@pytest.fixture(scope="session")
def dbt_profile_target(request, workspace_id, api_endpoint, schema_mode):
    """Build a dbt profile target from orchestrator-provided env vars.

    Enables ``reuse_session=True`` and ``session_id_file`` so all xdist
    workers share a single Livy session per lakehouse.
    """
    lakehouse_id = os.getenv("LAKEHOUSE_ID")
    lakehouse_name = os.getenv("LAKEHOUSE_NAME")
    schema_name = os.getenv("SCHEMA_NAME")

    if not all([lakehouse_id, lakehouse_name]):
        pytest.skip("LAKEHOUSE_ID / LAKEHOUSE_NAME not set — run via orchestrator or set manually")

    profile_type = request.config.getoption("--profile", default="az_cli")
    session_id_file = request.config.getoption("--session-id-file", default=None)
    session_id_files = request.config.getoption("--session-id-files", default=None)

    # If multiple session files are provided, shard xdist workers across them.
    # ``loadscope`` keeps all tests of a class on one worker, so each class is
    # pinned to a single Livy session deterministically.
    if session_id_files:
        try:
            with open(session_id_files) as _f:
                shard_paths = [ln.strip() for ln in _f if ln.strip()]
        except OSError:
            shard_paths = []
        if shard_paths:
            worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
            try:
                worker_idx = int(worker.removeprefix("gw"))
            except ValueError:
                worker_idx = 0
            session_id_file = shard_paths[worker_idx % len(shard_paths)]

    base = {
        "type": "fabricspark",
        "method": "livy",
        "connect_retries": 5,
        "connect_timeout": 15,
        "endpoint": api_endpoint,
        "workspaceid": workspace_id,
        "lakehouseid": lakehouse_id,
        "lakehouse": lakehouse_name,
        "schema": schema_name or lakehouse_name,
        "retry_all": True,
        "create_shortcuts": False,
        "shortcuts_json_str": os.getenv("SHORTCUTS_JSON_STR"),
        "environmentId": os.getenv("FABRIC_ENVIRONMENT_ID"),
        "livy_mode": os.getenv("LIVY_MODE", "fabric"),
        "reuse_session": True,
        "session_idle_timeout": "60m",
        "spark_config": {
            "name": f"dbt-test-{lakehouse_name}",
            "tags": {
                "project": f"dbt-test-{lakehouse_name}",
            },
        },
    }

    if session_id_file:
        base["session_id_file"] = session_id_file

    if profile_type == "int_tests":
        base["authentication"] = "int_tests"
        base["accessToken"] = os.getenv("FABRIC_INTEGRATION_TESTS_TOKEN")
    else:
        base["authentication"] = "CLI"

    return base


@pytest.fixture(scope="class")
def dbt_profile_data(
    request, unique_schema, dbt_profile_target, profiles_config_update, is_schema_enabled
):
    """Build profile data with per-class schema isolation.

    Overrides root conftest's ``dbt_profile_data`` to:
    1. Use ``is_schema_enabled`` directly — no Fabric API call needed.
    2. Deep-copy the session-scoped target to prevent cross-class mutation.
    3. Schema-enabled lakehouses use ``unique_schema`` for test isolation.
    4. Non-schema lakehouses use identifier prefixes for parallel isolation;
       each class gets a unique prefix derived from ``unique_schema``.
    """
    target = copy.deepcopy(dbt_profile_target)

    if is_schema_enabled:
        target["schema"] = unique_schema
        target["identifier_prefix"] = ""
    else:
        target["schema"] = target.get("lakehouse")
        class_nodeid = f"{request.cls.__module__}::{request.cls.__qualname__}"
        token = group_token_for(class_nodeid, unique_schema)
        target["identifier_prefix"] = f"{token}_"

    # Set the adapter-wide prefix ClassVar immediately so it is available
    # during dbt parsing (before connections.open sets it).  Without this,
    # manifest nodes get un-prefixed identifiers while physical tables are
    # prefixed, causing catalog and cache mismatches.
    from dbt.adapters.fabricspark.relation import FabricSparkRelation

    FabricSparkRelation._identifier_prefix = target["identifier_prefix"]

    profile = {
        "test": {
            "outputs": {
                "default": target,
            },
            "target": "default",
        },
    }

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile


@pytest.fixture(scope="class", autouse=True)
def _prefix_aware_run_sql(project, is_schema_enabled, dbt_profile_data):
    """Monkey-patch ``project.run_sql`` to apply the identifier prefix.

    Upstream dbt test fixtures use ``run_sql`` / ``run_sql_file`` with SQL
    templates like ``INSERT INTO {schema}.seed ...``.  After formatting,
    ``{schema}`` becomes the lakehouse name, but the actual table was created
    with the identifier prefix (e.g. ``prefix_seed``).

    This fixture intercepts ``run_sql`` to rewrite ``schema.table`` references
    to ``schema.prefix_table`` when in ``no_schema`` mode.  The patch is a
    no-op in ``with_schema`` mode.
    """
    if is_schema_enabled:
        yield
        return

    target = dbt_profile_data["test"]["outputs"]["default"]
    prefix = target.get("identifier_prefix", "")
    schema_name = target.get("schema", "")
    if not prefix or not schema_name:
        yield
        return

    import re

    escaped_schema = re.escape(schema_name)
    _table_ref_re = re.compile(rf"({escaped_schema})\.(`?)(\w+)")

    def _rewrite_sql(sql: str) -> str:
        """Add identifier prefix to ``schema.table`` references in raw SQL."""

        def _replacer(m):
            s, bt, table = m.group(1), m.group(2), m.group(3)
            if table.startswith(prefix):
                return m.group(0)  # already prefixed
            return f"{s}.{bt}{prefix}{table}"

        return _table_ref_re.sub(_replacer, sql)

    original_run_sql = project.run_sql

    def _patched_run_sql(sql, fetch=None):
        # Pre-format the SQL (same transform that run_sql_with_adapter does)
        creds = project.adapter.config.credentials
        try:
            sql = sql.format(
                schema=creds.schema,
                database=project.adapter.quote(creds.database) if creds.database else "",
            )
        except (KeyError, IndexError, ValueError):
            pass
        # Apply identifier prefix to table references
        sql = _rewrite_sql(sql)
        # Delegate to original — the format call inside is a no-op now
        return original_run_sql(sql, fetch=fetch)

    project.run_sql = _patched_run_sql
    yield
    project.run_sql = original_run_sql
