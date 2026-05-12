"""End-to-end smoke test for the `token_credential` auth method.

Run locally against your own Fabric workspace:

    uv run pytest --profile token_credential \
        tests/functional/adapter/test_token_credential.py

CI does not exercise this profile — it's intended for contributor smoke
testing per the contrib workflow (discussion #166).
"""

import pytest
from azure.core.credentials import AccessToken, TokenCredential

from dbt.tests.util import run_dbt


class AzureCliBackedCredential(TokenCredential):
    """Thin TokenCredential that delegates to AzureCliCredential.

    Exists so the functional smoke test has a real, importable dotted-path
    credential that produces a valid Fabric token without requiring the
    contributor to stand up a separate broker. Mimics the shape of what a
    real user-supplied credential would look like.
    """

    def __init__(self, **kwargs):
        from azure.identity import AzureCliCredential

        self._inner = AzureCliCredential(**kwargs)

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        return self._inner.get_token(*scopes, **kwargs)


@pytest.mark.skip_profile("az_cli", "azure_spn", "int_tests")
class TestTokenCredentialAuth:
    """Smoke test: a custom TokenCredential drives a real Fabric Livy session."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"hello.sql": "select 1 as id"}

    def test_dbt_run_under_token_credential(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
