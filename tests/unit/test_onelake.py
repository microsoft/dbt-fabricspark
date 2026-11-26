from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import requests
from azure.core.credentials import AccessToken
from dbt_common.exceptions import DbtRuntimeError
from jinja2 import Environment

from dbt.adapters.fabricspark import onelake
from dbt.adapters.fabricspark.impl import FabricSparkAdapter


@pytest.fixture
def creds():
    return SimpleNamespace(
        workspaceid="local-ws",
        lakehouseid="local-item",
        lakehouse="Main",
        authentication="CLI",
        endpoint="https://api.fabric.microsoft.com/v1",
        connect_retries=1,
        http_timeout=10,
    )


@pytest.fixture
def relation():
    return SimpleNamespace(
        database="Main",
        schema="Reports",
        identifier="pre_Daily #1",
        workspace=None,
        include_policy=SimpleNamespace(database=True),
    )


def response(payload=None, status=200):
    result = Mock(status_code=status, headers={})
    result.json.return_value = payload
    return result


def test_merge_preserves_all_other_metadata_and_reconciles_timeout(creds, relation, monkeypatch):
    before = {
        "description": "Unmanaged",
        "tags": {"owner": "BI"},
        "columns": [
            {"columnId": 1, "columnName": "id", "tags": {"role": "key"}},
            {"columnName": "extra", "description": "Manual"},
        ],
    }
    after = {
        **before,
        "columns": [
            {"columnName": "id", "tags": {"role": "key"}, "description": "ID"},
            before["columns"][1],
        ],
    }
    get = Mock(side_effect=[response(before), response(after)])
    post = Mock(side_effect=requests.Timeout())
    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setattr(onelake, "_wait", Mock())
    with patch.object(
        onelake.livysession, "get_cli_access_token", return_value=AccessToken("storage", 1)
    ) as token:
        original = onelake.livysession.accessToken
        onelake.persist_descriptions(creds, relation, None, {"id": "ID"})
        token.assert_called_once_with(creds, onelake.STORAGE_SCOPE)
        assert onelake.livysession.accessToken is original
    post.assert_called_once_with(
        onelake.metadata_url(creds, relation),
        headers={"Accept": "application/json", "Authorization": "Bearer storage"},
        json=after,
        timeout=10,
    )
    assert get.call_count == 2


def test_retry_remerges_concurrent_edits(creds, relation, monkeypatch):
    latest = {"tags": {"new": "tag"}, "columns": [{"columnName": "manual", "description": "kept"}]}
    expected = {**latest, "description": "x" * 1024}
    monkeypatch.setattr(onelake, "_headers", Mock(return_value={}))
    monkeypatch.setattr(onelake, "_wait", Mock())
    monkeypatch.setattr(
        requests, "get", Mock(side_effect=[response({}), response(latest), response(expected)])
    )
    post = Mock(side_effect=[response(status=503), response(expected)])
    monkeypatch.setattr(requests, "post", post)
    onelake.persist_descriptions(creds, relation, expected["description"], {})
    assert post.call_args.kwargs["json"] == expected


@pytest.mark.parametrize(
    "bad",
    [
        response(status=403),
        response([]),
        response({"columns": {}}),
        response({"columns": [{"name": "id"}]}),
    ],
)
def test_never_writes_after_failed_read_or_invalid_metadata(creds, relation, monkeypatch, bad):
    monkeypatch.setattr(onelake, "_headers", Mock(return_value={}))
    monkeypatch.setattr(requests, "get", Mock(return_value=bad))
    post = Mock()
    monkeypatch.setattr(requests, "post", post)
    with pytest.raises(DbtRuntimeError):
        onelake.persist_descriptions(creds, relation, "new", {})
    post.assert_not_called()


@pytest.mark.parametrize(
    ("description", "columns"),
    [("x" * 1025, {}), (None, {"c": "x" * 1025})],
    ids=["table", "column"],
)
def test_oversized_descriptions_fail_before_io(creds, relation, description, columns):
    with (
        patch.object(onelake, "_headers") as headers,
        pytest.raises(DbtRuntimeError, match="1024"),
    ):
        onelake.persist_descriptions(creds, relation, description, columns)
    headers.assert_not_called()


def test_cross_workspace_and_flat_lakehouse_routes(creds, relation, monkeypatch):
    relation.workspace = "Other"
    relation.schema = "report/s"
    get = Mock(
        side_effect=[
            response({"value": [], "continuationToken": "next"}),
            response({"value": [{"displayName": "Other", "id": "other-ws"}]}),
            response({"value": [{"displayName": "Main", "id": "other-item"}]}),
        ]
    )
    monkeypatch.setattr(requests, "get", get)
    with patch.object(
        onelake.livysession, "get_cli_access_token", return_value=AccessToken("fabric", 1)
    ) as token:
        assert onelake.metadata_url(creds, relation).endswith(
            "/other-ws/other-item/schemas/report%2Fs/tables/pre_Daily%20%231/additionalMetadata"
        )
        token.assert_called_once_with(creds, onelake.livysession.AZURE_CREDENTIAL_SCOPE)
    assert get.call_args_list[1].kwargs["params"] == {"continuationToken": "next"}
    assert get.call_args_list[2].args[0].endswith("/workspaces/other-ws/lakehouses")
    relation.include_policy.database = False
    relation.schema = "Main"
    assert "/local-ws/local-item/schemas/dbo/" in onelake.metadata_url(creds, relation)


def test_ambiguous_workspace_and_raw_token_fail(creds, relation, monkeypatch):
    relation.workspace = "Duplicate"
    monkeypatch.setattr(
        requests,
        "get",
        Mock(
            return_value=response(
                {"value": [{"displayName": "Duplicate", "id": i} for i in ("one", "two")]}
            )
        ),
    )
    with (
        patch.object(onelake, "_headers", return_value={}),
        pytest.raises(DbtRuntimeError, match="found 2"),
    ):
        onelake.metadata_url(creds, relation)
    creds.authentication = "int_tests"
    with pytest.raises(DbtRuntimeError, match="raw Fabric token"):
        onelake.persist_descriptions(creds, relation, "new", {})


@pytest.mark.parametrize(
    ("opt_in", "table", "cols"),
    [(False, True, True), (True, False, False), (True, True, False), (True, False, True)],
)
def test_persist_table_docs_selection(opt_in, table, cols):
    env = Environment(extensions=["jinja2.ext.do"])
    macro = env.from_string(Path("src/dbt/include/fabricspark/macros/adapters.sql").read_text())
    config = Mock()
    config.get.return_value = opt_in
    config.persist_relation_docs.return_value = table
    config.persist_column_docs.return_value = cols
    adapter, spark = Mock(), Mock()
    model = SimpleNamespace(description="table docs", columns={"col": {"description": "col docs"}})
    macro.make_module(
        {"adapter": adapter, "config": config, "persist_docs": spark}
    ).fabricspark_persist_table_docs("relation", model)
    spark.assert_called_once_with("relation", model)
    if opt_in and (table or cols):
        adapter.persist_onelake_descriptions.assert_called_once_with(
            "relation", model.description if table else None, model.columns if cols else {}
        )
    else:
        adapter.persist_onelake_descriptions.assert_not_called()


def test_non_delta_provider_skips_metadata(relation):
    adapter = Mock()
    adapter.execute.return_value = (None, SimpleNamespace(rows=[("Provider", "parquet")]))
    with patch.object(onelake, "persist_descriptions") as sync:
        FabricSparkAdapter.persist_onelake_descriptions(adapter, relation, "desc", {})
        sync.assert_not_called()


def test_mlv_comment_renders_spark_literal():
    path = Path(
        "src/dbt/include/fabricspark/macros/materializations/models/materialized_lake_view/materialized_lake_view.sql"
    )
    clause = next(
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip().startswith("COMMENT ")
    )
    assert (
        Environment().from_string(clause).render(mlv_comment="Analyst's data")
        == r"COMMENT 'Analyst\'s data'"
    )
