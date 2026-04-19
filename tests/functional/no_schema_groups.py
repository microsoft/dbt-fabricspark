"""Central registry for no_schema-mode identifier grouping.

In ``with_schema`` mode each test class gets its own dbt schema via
``unique_schema``.  In ``no_schema`` mode there is only one namespace, so we
prefix every dbt-managed identifier with a per-group token to avoid
cross-class collisions.

By default, group == class and the token == that class's ``unique_schema``
value (the same random+epoch string dbt would have used as a schema name).
Register classes here only when they must share state with other classes.

Keys are pytest *class* nodeids in the form
    ``"<module>::<Class>"``
    e.g. ``"tests.functional.adapter.basic.test_base::TestSimpleMaterializations"``
Values are group tokens shared across all listed classes.

This file is the single source of truth; tests never compute prefixes
on their own and no macro is involved.
"""

from __future__ import annotations

# Explicit overrides.  Omit a class to accept the default (class == group).
# Example of a future shared group:
#   "tests.functional.adapter.basic.test_base::TestSimpleMaterializations": "basic_mat",
#   "tests.functional.adapter.basic.test_incremental::TestIncremental":     "basic_mat",
NO_SCHEMA_GROUPS: dict[str, str] = {}


def group_token_for(class_nodeid: str, default_token: str) -> str:
    """Return the group token for a given pytest class nodeid.

    ``default_token`` should be the class's own ``unique_schema`` so that
    unregistered classes remain fully isolated.
    """
    return NO_SCHEMA_GROUPS.get(class_nodeid, default_token)
