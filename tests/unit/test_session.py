import datetime as dt
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.fabricspark.connections import FabricSparkConnectionManager
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.relation import FabricSparkRelation
from dbt.adapters.fabricspark.session import (
    SessionConnection,
    SessionConnectionWrapper,
    SessionCursor,
    _dbt_job_description,
    _load_pyspark,
)


class FakeAnalysisException(Exception):
    pass


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            '/* {\n  "app": "dbt",\n  "node_id": "model.example.orders"\n} */\nselect 1',
            "model.example.orders",
        ),
        (
            '/* {"app": "dbt", "node_id": 42, "connection_name": "master"} */ select 1',
            "master",
        ),
        ('/* {"app": "dbt", "node_id": invalid} */ select 1', "dbt query"),
        ('/* {"app": "other", "node_id": "model.example.orders"} */ select 1', "dbt query"),
        ("select 1", "dbt query"),
    ],
)
def test_dbt_job_description(sql: str, expected: str) -> None:
    assert _dbt_job_description(sql) == expected


def test_credentials_session_mode() -> None:
    with patch("dbt.adapters.fabricspark.credentials.import_module", return_value=object()):
        credentials = FabricSparkCredentials(
            method="session",
            spark_config={
                "name": "dbt-session",
                "conf": {"spark.master": "local[2]", "spark.sql.ansi.enabled": True},
            },
        )

    assert credentials.is_session_method is True
    assert credentials.is_local_mode is True
    assert credentials.workspaceid is None
    assert credentials.lakehouseid is None
    assert credentials.schema == "default"
    assert credentials.database == "default"
    assert credentials.unique_field == "session:dbt-session"
    assert credentials.spark_config["conf"] == {
        "spark.master": "local[2]",
        "spark.sql.ansi.enabled": "false",
    }


@pytest.mark.parametrize(
    ("schema", "schemas_enabled"),
    [
        ("silver", True),
        ("SilverLakehouse", False),
    ],
)
def test_session_credentials_infer_schema_mode(schema: str, schemas_enabled: bool) -> None:
    with patch("dbt.adapters.fabricspark.credentials.import_module", return_value=object()):
        credentials = FabricSparkCredentials(
            method="session",
            workspace_name="AnalyticsWorkspace",
            lakehouse="SilverLakehouse",
            schema=schema,
            spark_config={"name": "dbt-session"},
        )

    assert credentials.lakehouse_schemas_enabled is schemas_enabled


def test_session_pyspark_imports_supported_api() -> None:
    spark_session_type, analysis_error = _load_pyspark()

    assert spark_session_type.__name__ == "SparkSession"
    assert issubclass(analysis_error, Exception)


def test_credentials_session_mode_requires_pyspark() -> None:
    with (
        patch(
            "dbt.adapters.fabricspark.credentials.import_module",
            side_effect=ImportError("No module named 'pyspark'"),
        ),
        pytest.raises(DbtRuntimeError, match=r"dbt-fabricspark\[spark\]"),
    ):
        FabricSparkCredentials(
            method="session",
            spark_config={"name": "dbt-session"},
        )


def test_credentials_session_mode_requires_mapping_conf() -> None:
    with pytest.raises(ValueError, match="spark_config.conf must be a mapping"):
        FabricSparkCredentials(
            method="session",
            spark_config={"name": "dbt-session", "conf": ["not", "a", "mapping"]},
        )


def test_credentials_session_mode_requires_spark_config_name() -> None:
    with pytest.raises(ValueError, match="Missing required key: name"):
        FabricSparkCredentials(method="session", spark_config={"conf": {}})


def _builder() -> MagicMock:
    builder = MagicMock()
    builder.config.return_value = builder
    builder.appName.return_value = builder
    builder.enableHiveSupport.return_value = builder
    return builder


def test_session_connection_builds_hive_session_from_spark_config() -> None:
    builder = _builder()
    spark_session = MagicMock()
    builder.getOrCreate.return_value = spark_session
    spark_session_type = SimpleNamespace(builder=builder)

    with patch(
        "dbt.adapters.fabricspark.session._load_pyspark",
        return_value=(spark_session_type, FakeAnalysisException),
    ):
        connection = SessionConnection(
            spark_config={
                "name": "dbt-session",
                "conf": {
                    "spark.master": "local[2]",
                    "spark.sql.ansi.enabled": "false",
                },
            }
        )

    assert builder.config.call_args_list == [
        call("spark.master", "local[2]"),
        call("spark.sql.ansi.enabled", "false"),
    ]
    builder.appName.assert_called_once_with("dbt-session")
    builder.enableHiveSupport.assert_called_once_with()
    builder.getOrCreate.assert_called_once_with()
    assert connection.cursor()._spark_session is spark_session
    connection.close()
    spark_session.stop.assert_not_called()


def test_session_cursor_executes_and_fetches_rows() -> None:
    spark_session = MagicMock()
    data_type = MagicMock()
    data_type.simpleString.return_value = "array<string>"
    field = SimpleNamespace(name="items", dataType=data_type, nullable=False)
    dataframe = MagicMock()
    dataframe.schema.fields = [field]
    dataframe.collect.return_value = [("first",), ("second",), ("third",)]
    spark_session.sql.return_value = dataframe
    cursor = SessionCursor(spark_session, FakeAnalysisException)

    cursor.execute("select %s", 1)

    spark_session.sql.assert_called_once_with("select 1")
    assert cursor.description == [("items", "array<string>", None, None, None, None, False)]
    assert cursor.fetchone() == ("first",)
    assert cursor.fetchmany(1) == [("second",)]
    assert cursor.fetchall() == [("first",), ("second",), ("third",)]
    assert cursor.fetchone() == ("third",)
    assert cursor.fetchone() is None

    cursor.close()
    assert cursor.description == []
    assert cursor.fetchall() is None


def test_session_cursor_labels_eager_and_lazy_spark_work() -> None:
    spark_context = MagicMock()
    spark_context.getLocalProperty.return_value = None
    dataframe = MagicMock()
    dataframe.collect.return_value = [(1,)]
    spark_session = MagicMock()
    spark_session.sparkContext = spark_context
    spark_session.sql.return_value = dataframe
    cursor = SessionCursor(spark_session, FakeAnalysisException)

    cursor.execute('/* {"app": "dbt", "node_id": "model.example.orders"} */ select 1')
    assert cursor.fetchall() == [(1,)]

    group_id = spark_context.setJobGroup.call_args_list[0].args[0]
    assert group_id.startswith("dbt:model.example.orders:")
    assert spark_context.setJobGroup.call_args_list == [
        call(group_id, "model.example.orders", interruptOnCancel=True),
        call(group_id, "model.example.orders", interruptOnCancel=True),
    ]
    cleanup = [
        call("spark.jobGroup.id", None),
        call("spark.job.description", None),
        call("spark.job.interruptOnCancel", None),
    ]
    assert spark_context.setLocalProperty.call_args_list == cleanup * 2


def test_session_cursor_restores_job_group_after_collect_failure() -> None:
    spark_context = MagicMock()
    spark_context.getLocalProperty.return_value = None
    dataframe = MagicMock()
    dataframe.collect.side_effect = RuntimeError("collect failed")
    spark_session = MagicMock()
    spark_session.sparkContext = spark_context
    spark_session.sql.return_value = dataframe
    cursor = SessionCursor(spark_session, FakeAnalysisException)
    cursor.execute('/* {"app": "dbt", "connection_name": "model.example.orders"} */ select 1')

    spark_context.reset_mock()
    spark_context.getLocalProperty.side_effect = [
        "outer-group",
        "outer description",
        "false",
    ]

    with pytest.raises(RuntimeError, match="collect failed"):
        cursor.fetchall()

    group_id = spark_context.setJobGroup.call_args.args[0]
    assert group_id.startswith("dbt:model.example.orders:")
    spark_context.setJobGroup.assert_called_once_with(
        group_id,
        "model.example.orders",
        interruptOnCancel=True,
    )
    assert spark_context.setLocalProperty.call_args_list == [
        call("spark.jobGroup.id", "outer-group"),
        call("spark.job.description", "outer description"),
        call("spark.job.interruptOnCancel", "false"),
    ]


def test_session_cursor_translates_analysis_errors_and_clears_previous_result() -> None:
    spark_session = MagicMock()
    cursor = SessionCursor(spark_session, FakeAnalysisException)
    cursor._df = MagicMock()
    spark_session.sql.side_effect = FakeAnalysisException("missing table")

    with pytest.raises(DbtRuntimeError, match="missing table"):
        cursor.execute("select * from missing")

    assert cursor.description == []


def test_session_cursor_context_manager_does_not_suppress_errors() -> None:
    cursor = SessionCursor(MagicMock(), FakeAnalysisException)

    with pytest.raises(RuntimeError, match="boom"):
        with cursor:
            raise RuntimeError("boom")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("1.25"), 1.25),
        (dt.datetime(2026, 8, 23, 19, 52, 55, 414000), "'2026-08-23 19:52:55.414'"),
        (None, "''"),
        ("O'Brien", "'O\\'Brien'"),
    ],
)
def test_session_wrapper_fixes_bindings(value, expected) -> None:
    assert SessionConnectionWrapper._fix_binding(value) == expected


def test_session_wrapper_strips_semicolon_and_passes_bindings() -> None:
    cursor = MagicMock()
    handle = MagicMock()
    handle.cursor.return_value = cursor
    wrapper = SessionConnectionWrapper(handle).cursor()

    wrapper.execute("select %s, %s;", [Decimal("1.5"), "value"])

    cursor.execute.assert_called_once_with("select %s, %s", 1.5, "'value'")


def test_connection_manager_routes_session_without_fabric_or_livy() -> None:
    with patch("dbt.adapters.fabricspark.credentials.import_module", return_value=object()):
        credentials = FabricSparkCredentials(
            method="session",
            lakehouse="dbt_session_e2e",
            schema="dbt_session_e2e",
            spark_config={"name": "dbt-session", "conf": {"spark.master": "local[2]"}},
        )
    connection = Connection(
        type="fabricspark",
        name="session-test",
        credentials=credentials,
    )
    raw_handle = MagicMock()

    with (
        patch("dbt.adapters.fabricspark.connections.get_lakehouse_properties") as get_properties,
        patch(
            "dbt.adapters.fabricspark.session.SessionConnection",
            return_value=raw_handle,
        ) as session_connection,
        patch.object(FabricSparkConnectionManager, "fetch_spark_version"),
        patch.object(FabricSparkConnectionManager, "check_mlv_prerequisites"),
    ):
        opened = FabricSparkConnectionManager.open(connection)

    assert opened.state == ConnectionState.OPEN
    assert isinstance(opened.handle, SessionConnectionWrapper)
    session_connection.assert_called_once_with(spark_config=credentials.spark_config)
    get_properties.assert_not_called()


def test_connection_manager_propagates_inferred_session_schema_mode() -> None:
    with patch("dbt.adapters.fabricspark.credentials.import_module", return_value=object()):
        credentials = FabricSparkCredentials(
            method="session",
            workspace_name="AnalyticsWorkspace",
            lakehouse="SilverLakehouse",
            schema="silver",
            spark_config={"name": "dbt-session"},
        )
    connection = Connection(
        type="fabricspark",
        name="session-test",
        credentials=credentials,
    )

    try:
        with (
            patch(
                "dbt.adapters.fabricspark.connections.get_lakehouse_properties"
            ) as get_properties,
            patch(
                "dbt.adapters.fabricspark.session.SessionConnection",
                return_value=MagicMock(),
            ),
            patch.object(FabricSparkConnectionManager, "fetch_spark_version"),
            patch.object(FabricSparkConnectionManager, "check_mlv_prerequisites"),
        ):
            FabricSparkConnectionManager.open(connection)

        assert FabricSparkRelation._schemas_enabled is True
        assert (
            str(
                FabricSparkRelation.create(
                    database="SilverLakehouse",
                    schema="silver",
                    identifier="model",
                    workspace="AnalyticsWorkspace",
                )
            )
            == "`AnalyticsWorkspace`.`SilverLakehouse`.`silver`.model"
        )
        get_properties.assert_not_called()
    finally:
        FabricSparkRelation._schemas_enabled = False
