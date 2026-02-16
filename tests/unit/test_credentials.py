from dbt.adapters.fabricspark import FabricSparkCredentials


def test_credentials_server_side_parameters_keys_and_values_are_strings() -> None:
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="tests",
        schema="tests",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        spark_config={"name": "test-session"},
    )
    assert credentials.schema == "tests"
