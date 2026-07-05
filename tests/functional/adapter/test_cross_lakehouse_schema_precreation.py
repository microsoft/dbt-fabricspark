"""Schema pre-creation must land only in the configured target lakehouse, never
additionally in the session-bound one.

Drives the real ``create_schema`` / ``drop_schema`` against live Fabric with the
stale relation ``before_run`` produces (database segment dropped) and asserts the
schema is created/dropped in target lakehouse B and never in session-bound A.
"""

from __future__ import annotations

import uuid

import pytest

PLACEHOLDER_MODEL_SQL = "select 1 as id"


def _schema_exists(adapter, qualified_schema: str) -> bool:
    """True iff ``DESCRIBE SCHEMA`` succeeds; a not-found error means False."""
    try:
        adapter.execute(f"describe schema {qualified_schema}", auto_begin=False, fetch=True)
        return True
    except Exception as exc:  # noqa: BLE001
        msg = str(getattr(exc, "msg", exc)).lower()
        if (
            "schema_not_found" in msg
            or "nosuchnamespace" in msg
            or "not found" in msg
            or "does not exist" in msg
        ):
            return False
        raise


class TestCrossLakehouseSchemaPreCreation:
    """Schema DDL for a ``+database`` target must not touch the session-bound lakehouse."""

    @pytest.fixture(scope="class", autouse=True)
    def _skip_unless_schema_enabled(self, is_schema_enabled):
        if not is_schema_enabled:
            pytest.skip("Cross-lakehouse +database routing requires schema-enabled mode.")

    @pytest.fixture(scope="class")
    def models(self):
        return {"_placeholder.sql": PLACEHOLDER_MODEL_SQL}

    def test_create_and_drop_schema_route_only_to_target_lakehouse(
        self,
        project,
        second_lakehouse_name,
    ):
        adapter = project.adapter
        session_bound_lakehouse = adapter.config.credentials.lakehouse
        target_lakehouse = second_lakehouse_name
        assert target_lakehouse.casefold() != session_bound_lakehouse.casefold()

        custom_schema = f"xlh_{uuid.uuid4().hex[:12]}"

        # Stale relation before_run builds: target B, database segment dropped.
        stale_relation = (
            adapter.Relation.create(
                database=target_lakehouse,
                schema=custom_schema,
                identifier="placeholder",
            )
            .without_identifier()
            .include(database=False)
        )
        assert not stale_relation.include_policy.database

        qualified_target = f"`{target_lakehouse}`.`{custom_schema}`"
        # Unqualified resolves against the session-bound lakehouse.
        unqualified_session_bound = f"`{custom_schema}`"

        with adapter.connection_named("cross_lakehouse_schema_ddl"):
            try:
                adapter.create_schema(stale_relation)
                assert not _schema_exists(adapter, unqualified_session_bound), (
                    f"schema {custom_schema!r} was wrongly created in session-bound "
                    f"lakehouse {session_bound_lakehouse!r}"
                )
                assert _schema_exists(adapter, qualified_target), (
                    f"schema {custom_schema!r} was not created in target lakehouse "
                    f"{target_lakehouse!r}"
                )

                adapter.drop_schema(stale_relation)
                assert not _schema_exists(adapter, qualified_target), (
                    f"drop_schema did not remove {custom_schema!r} from {target_lakehouse!r}"
                )
            finally:
                for qualified in (qualified_target, unqualified_session_bound):
                    try:
                        adapter.execute(
                            f"drop database if exists {qualified} cascade",
                            auto_begin=False,
                            fetch=False,
                        )
                    except Exception:  # noqa: BLE001
                        pass
