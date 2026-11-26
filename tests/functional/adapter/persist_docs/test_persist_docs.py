import pytest
import requests
from azure.identity import AzureCliCredential
from fixtures import (
    _MODELS__INCREMENTAL_DELTA,
    _MODELS__MY_FUN_DOCS,
    _MODELS__TABLE_DELTA_MODEL,
    _MODELS__TABLE_DELTA_MODEL_MISSING_COLUMN,
    _MODELS__VIEW_DELTA_MODEL,
    _PROPERTIES__MODELS,
    _PROPERTIES__SEEDS,
    _SEEDS__BASIC,
    _VIEW_PROPERTIES_MODELS,
)

from dbt.adapters.fabricspark.onelake import table_api_base
from dbt.tests.util import run_dbt


class TestPersistDocsDeltaTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_delta_model.sql": _MODELS__INCREMENTAL_DELTA,
            "my_fun_docs.md": _MODELS__MY_FUN_DOCS,
            "table_delta_model.sql": _MODELS__TABLE_DELTA_MODEL,
            "schema.yml": _PROPERTIES__MODELS,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS__BASIC, "seed.yml": _PROPERTIES__SEEDS}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
            "seeds": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                    "+file_format": "delta",
                    "+quote_columns": True,
                }
            },
        }

    def test_delta_comments(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        for table, whatis in [
            ("table_delta_model", "Table"),
            ("seed", "Seed"),
            ("incremental_delta_model", "Incremental"),
        ]:
            results = project.run_sql(
                "describe extended {schema}.{table}".format(
                    schema=project.adapter.config.credentials.schema, table=table
                ),
                fetch="all",
            )

            for result in results:
                if result[0] == "Comment":
                    assert result[1].startswith(f"{whatis} model description")
                if result[0] == "id":
                    assert result[2].startswith("id Column description")
                if result[0] == "name":
                    assert result[2].startswith("Some stuff here and then a call to")


@pytest.mark.skip_profile("int_tests")
class TestOneLakePersistDocs:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"source.csv": _SEEDS__BASIC}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "doc_table.sql": "{{ config(materialized='table') }} select * from {{ ref('source') }}",
            "doc_incremental.sql": "{{ config(materialized='incremental') }} select * from {{ ref('doc_table') }}",
            "schema.yml": """
version: 2
seeds:
  - name: source
    description: Source description
models:
  - name: doc_table
    description: Table description
  - name: doc_incremental
    columns:
      - name: id
        description: Incremental identifier
""",
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot.yml": """
version: 2
snapshots:
  - name: doc_snapshot
    relation: ref('doc_table')
    description: Snapshot description
    config:
      strategy: check
      unique_key: id
      check_cols: [name]
"""
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            key: {
                "test": {
                    "+file_format": "delta",
                    "+persist_docs_onelake": True,
                    "+persist_docs": {"relation": True, "columns": True},
                }
            }
            for key in ("models", "seeds", "snapshots")
        }

    def test_metadata_materializations(self, project, is_schema_enabled):
        creds = project.adapter.config.credentials
        prefix = creds.identifier_prefix or ""
        schema = creds.schema if is_schema_enabled else "dbo"
        credential = AzureCliCredential(process_timeout=creds.azure_cli_process_timeout)

        def metadata(table, payload=None):
            if payload is not None:
                for entry in payload.get("columns", []):
                    entry.pop("columnId", None)
            url = (
                f"{table_api_base(creds.endpoint)}/{creds.workspaceid}/{creds.lakehouseid}"
                f"/schemas/{schema}/tables/{prefix}{table}/additionalMetadata"
            )
            token = credential.get_token("https://storage.azure.com/.default")
            response = requests.request(
                "POST" if payload is not None else "GET",
                url,
                headers={"Authorization": f"Bearer {token.token}"},
                json=payload,
                timeout=creds.http_timeout,
            )
            response.raise_for_status()
            return response.json()

        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["snapshot"])
        assert metadata("source")["description"] == "Source description"
        assert metadata("doc_table")["description"] == "Table description"
        incremental = metadata("doc_incremental")
        column = next(c for c in incremental["columns"] if c["columnName"] == "id")
        assert column["description"] == "Incremental identifier"
        incremental["description"] = "Retain manual table description"
        incremental["tags"] = {"owner": "functional-test"}
        column.update(description="Old identifier", tags={"role": "key"})
        metadata("doc_incremental", incremental)
        snapshot = metadata("doc_snapshot")
        assert snapshot["description"] == "Snapshot description"
        snapshot["description"] = "Old snapshot"
        metadata("doc_snapshot", snapshot)

        run_dbt(["run", "--select", "doc_incremental"])
        run_dbt(["snapshot"])
        result = metadata("doc_incremental")
        assert result["tags"] == incremental["tags"]
        assert result["description"] == incremental["description"]
        stored = next(c for c in result["columns"] if c["columnName"] == "id")
        assert stored["tags"] == column["tags"]
        assert stored["description"] == "Incremental identifier"
        assert metadata("doc_snapshot")["description"] == "Snapshot description"


class TestPersistDocsDeltaView:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_delta_model.sql": _MODELS__TABLE_DELTA_MODEL,
            "view_delta_model.sql": _MODELS__VIEW_DELTA_MODEL,
            "schema.yml": _VIEW_PROPERTIES_MODELS,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
        }

    def test_delta_comments(self, project):
        run_dbt(["run"])

        results = project.run_sql(
            "describe extended {schema}.{table}".format(
                schema=project.adapter.config.credentials.schema, table="view_delta_model"
            ),
            fetch="all",
        )

        for result in results:
            if result[0] == "Comment":
                assert result[1].startswith("View model description")
            if result[0] == "id":
                assert result[2].startswith("id Column description")
            if result[0] == "count":
                assert result[2] is None


class TestPersistDocsMissingColumn:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS__BASIC, "seed.yml": _PROPERTIES__SEEDS}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_delta_model.sql": _MODELS__TABLE_DELTA_MODEL_MISSING_COLUMN,
            "my_fun_docs.md": _MODELS__MY_FUN_DOCS,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": _PROPERTIES__MODELS}

    def test_missing_column(self, project):
        """spark will use our schema to verify all columns exist rather than fail silently"""
        run_dbt(["seed"])
        res = run_dbt(["run"], expect_pass=False)
        assert "Missing field name in table" in res[0].message
