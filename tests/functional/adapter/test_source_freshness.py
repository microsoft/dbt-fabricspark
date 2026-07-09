"""Functional test for `dbt source freshness` (#237).

Fabric's Livy statement API returns ``timestamp`` columns to Python as strings.
dbt-core's freshness path validates that ``max_loaded_at`` / ``snapshotted_at``
are native ``datetime`` objects, so before the adapter-level coercion fix a
``dbt source freshness`` run failed with:

    Expected a timestamp value when querying field '...' of table None but
    received value of type 'str' instead

This test seeds a table with a timestamp column, points a source at it with a
``loaded_at_field``, and asserts ``dbt source freshness`` completes and returns
native ``datetime`` values — exercising the exact path from the bug report.

Runs unchanged in both ``no_schema`` and ``with_schema`` modes: the source is
rendered through ``FabricSparkRelation`` with the same identifier prefix and
database-scoping rules as the seed, so ``{{ target.schema }}`` resolves to the
same location the seed was created in. ``database`` is intentionally omitted so
it defaults to the adapter's database (include/exclude is handled per mode).
"""

import datetime

import pytest

from dbt.artifacts.schemas.freshness import FreshnessStatus
from dbt.tests.util import run_dbt

_seed_csv = """id,loaded_at
1,2020-01-01 00:00:00
2,2020-01-02 12:30:00
3,2020-01-03 08:15:45
""".lstrip()

# Thresholds are intentionally huge so a fixed 2020 seed always resolves to
# `pass` — the point of the test is that freshness *completes* with native
# datetimes, not that the data is fresh.
_sources_yml = """
version: 2

sources:
  - name: freshness_source
    schema: "{{ target.schema }}"
    tables:
      - name: freshness_seed
        loaded_at_field: "cast(loaded_at as timestamp)"
        freshness:
          warn_after: {count: 8760000, period: hour}
          error_after: {count: 87600000, period: hour}
"""


class TestSourceFreshness:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "source_freshness"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"freshness_seed.csv": _seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"sources.yml": _sources_yml}

    def test_source_freshness_returns_native_datetimes(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1

        # expect_pass=True (default): before the fix this raised because the
        # freshness node errored on the string-typed timestamps.
        freshness = run_dbt(["source", "freshness"])
        assert len(freshness.results) == 1

        result = freshness.results[0]
        assert result.status == FreshnessStatus.Pass
        assert isinstance(result.max_loaded_at, datetime.datetime)
        assert isinstance(result.snapshotted_at, datetime.datetime)
        assert result.max_loaded_at.year == 2020
