import json
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

REPO_ROOT = Path(__file__).resolve().parents[2]


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


def test_merge_preserves_all_other_metadata_and_verifies_write(creds, relation, monkeypatch):
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
    post = Mock(return_value=response(after))
    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(requests, "post", post)
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


@pytest.mark.parametrize(
    ("outcome", "message"),
    [
        (requests.Timeout(), "unknown outcome .* may still complete"),
        (requests.ConnectionError(), "unknown outcome"),
        (response(status=408), "unknown outcome"),
        (response(status=503), "unknown outcome"),
        (response(status=429), "rejected"),
        (response(status=400), "rejected"),
    ],
)
def test_post_is_sent_once_and_unconfirmed_writes_fail(
    creds, relation, monkeypatch, outcome, message
):
    get = Mock(side_effect=[response({}), response({"description": "new"})])
    post = Mock(side_effect=[outcome])
    monkeypatch.setattr(onelake, "_headers", Mock(return_value={}))
    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(requests, "post", post)
    with pytest.raises(DbtRuntimeError, match=message):
        onelake.persist_descriptions(creds, relation, "new", {})
    assert post.call_count == 1
    assert get.call_count == 1


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


@pytest.mark.parametrize(
    ("endpoint", "base"),
    [
        (
            "https://api.fabric.microsoft.com/v1",
            "https://onelake.table.fabric.microsoft.com/common/v1",
        ),
        (
            "https://msitapi.fabric.microsoft.com/v1",
            "https://msit-onelake.table.fabric.microsoft.com/common/v1",
        ),
        (
            "https://dailyapi.fabric.microsoft.com/v1",
            "https://daily-onelake.table.fabric.microsoft.com/common/v1",
        ),
        (
            "https://dxtapi.fabric.microsoft.com/v1",
            "https://dxt-onelake.table.fabric.microsoft.com/common/v1",
        ),
        (
            "https://msitapi.fabric.microsoft.com.example.org/v1",
            "https://onelake.table.fabric.microsoft.com/common/v1",
        ),
    ],
)
def test_metadata_url_uses_fabric_environment(creds, relation, endpoint, base):
    creds.endpoint = endpoint
    assert onelake.metadata_url(creds, relation) == (
        f"{base}/local-ws/local-item/schemas/Reports/tables/pre_Daily%20%231/additionalMetadata"
    )


def test_cross_workspace_routes_and_encoded_continuation(creds, relation, monkeypatch):
    creds.endpoint = "https://msitapi.fabric.microsoft.com/v1"
    relation.workspace = "Other"
    relation.schema = "report/s"
    pages = iter(
        [
            {"value": [], "continuationToken": "ABCsMTAwMDAwLDA%3D"},
            {"value": [{"displayName": "Other", "id": "other-ws"}]},
            {"value": [{"displayName": "Main", "id": "other-item"}]},
        ]
    )
    sent = []

    def send(adapter, request, **kwargs):
        sent.append(request.url)
        result = requests.Response()
        result.status_code = 200
        result._content = json.dumps(next(pages)).encode()
        return result

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with patch.object(
        onelake.livysession, "get_cli_access_token", return_value=AccessToken("fabric", 1)
    ) as token:
        assert onelake.metadata_url(creds, relation) == (
            "https://msit-onelake.table.fabric.microsoft.com/common/v1/other-ws/other-item"
            "/schemas/report%2Fs/tables/pre_Daily%20%231/additionalMetadata"
        )
        token.assert_called_once_with(creds, onelake.livysession.AZURE_CREDENTIAL_SCOPE)
    assert sent == [
        "https://msitapi.fabric.microsoft.com/v1/workspaces",
        "https://msitapi.fabric.microsoft.com/v1/workspaces?continuationToken=ABCsMTAwMDAwLDA%3D",
        "https://msitapi.fabric.microsoft.com/v1/workspaces/other-ws/lakehouses",
    ]
    relation.include_policy.database = False
    relation.schema = "Main"
    assert "/local-ws/local-item/schemas/dbo/" in onelake.metadata_url(creds, relation)


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://api.fabric.microsoft.com/v1",
        "https://api.fabric.microsoft.com.evil.example/v1",
        "https://localhost:8998",
    ],
)
@pytest.mark.parametrize("workspace", [None, "Other"], ids=["lakehouse", "workspace"])
def test_discovery_rejects_untrusted_origin_before_token(
    creds, relation, monkeypatch, endpoint, workspace
):
    creds.endpoint = endpoint
    relation.workspace = workspace
    relation.database = "Other"
    get = Mock()
    monkeypatch.setattr(requests, "get", get)
    with (
        patch.object(onelake.livysession, "get_cli_access_token") as token,
        pytest.raises(DbtRuntimeError, match="refusing to send a Fabric token"),
    ):
        onelake.metadata_url(creds, relation)
    token.assert_not_called()
    get.assert_not_called()


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
    macro = env.from_string(
        (REPO_ROOT / "src/dbt/include/fabricspark/macros/adapters.sql").read_text(encoding="utf-8")
    )
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
    path = (
        REPO_ROOT
        / "src/dbt/include/fabricspark/macros/materializations/models/materialized_lake_view/materialized_lake_view.sql"
    )
    clause = next(
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("COMMENT ")
    )
    assert (
        Environment().from_string(clause).render(mlv_comment=r"Analyst's \' literal \n")
        == r"COMMENT 'Analyst\'s \\\' literal \\n'"
    )
