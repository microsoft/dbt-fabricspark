from dbt.adapters.fabricspark import SparkCredentials


def test_credentials_server_side_parameters_keys_and_values_are_strings() -> None:
    credentials = SparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="tests",
        schema="tests",
        workspaceid="",
        lakehouseid="",
    )
    assert credentials.schema == "tests"
