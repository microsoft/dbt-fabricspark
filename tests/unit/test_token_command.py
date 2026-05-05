"""Tests for the ``authentication: token_command`` auth method.

Modelled on AWS CLI's ``credential_process`` pattern: dbt spawns a
user-configured command, reads stdout, and uses the returned token
until it nears expiry. The adapter parses JSON when possible and falls
back to raw-text mode otherwise. Token / expiry locations are
configurable via dot-notation paths.
"""
from __future__ import annotations

import json
import subprocess
import time
from unittest.mock import MagicMock, patch

import pytest
from azure.core.credentials import AccessToken
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livysession import (
    _resolve_token_command_args,
    _walk_json_path,
    get_headers,
    get_token_command_access_token,
)


# ---------------------------------------------------------------------------
# _resolve_token_command_args — input normalization
# ---------------------------------------------------------------------------


class TestResolveTokenCommandArgs:
    def test_list_of_strings_passes_through(self):
        assert _resolve_token_command_args(["a", "b", "c"]) == ["a", "b", "c"]

    def test_list_with_non_strings_is_coerced(self):
        # YAML can produce ints in a list (e.g. an arg that's a port number)
        assert _resolve_token_command_args(["a", 42]) == ["a", "42"]

    def test_empty_list_raises(self):
        with pytest.raises(DbtRuntimeError, match="empty list"):
            _resolve_token_command_args([])

    def test_string_is_shell_split(self):
        assert _resolve_token_command_args("/bin/get-token --user me") == [
            "/bin/get-token",
            "--user",
            "me",
        ]

    def test_string_preserves_quoted_args(self):
        assert _resolve_token_command_args('/bin/get-token --user "alice b"') == [
            "/bin/get-token",
            "--user",
            "alice b",
        ]

    def test_unparseable_string_raises(self):
        with pytest.raises(DbtRuntimeError, match="not a valid shell expression"):
            _resolve_token_command_args('"unclosed quote')

    def test_other_types_rejected(self):
        with pytest.raises(DbtRuntimeError, match="must be a string or list"):
            _resolve_token_command_args(42)


# ---------------------------------------------------------------------------
# _walk_json_path — dotted-path lookup helper
# ---------------------------------------------------------------------------


class TestWalkJsonPath:
    def test_top_level(self):
        assert _walk_json_path({"a": "x"}, "a") == "x"

    def test_nested(self):
        assert _walk_json_path({"data": {"token": "x"}}, "data.token") == "x"

    def test_deeply_nested(self):
        assert _walk_json_path({"a": {"b": {"c": 7}}}, "a.b.c") == 7

    def test_missing_returns_none(self):
        assert _walk_json_path({"a": 1}, "b") is None

    def test_through_non_dict_returns_none(self):
        assert _walk_json_path({"a": "string"}, "a.b") is None

    def test_camel_case_key(self):
        # Azure CLI default JSON output uses camelCase
        assert _walk_json_path({"accessToken": "x"}, "accessToken") == "x"


# ---------------------------------------------------------------------------
# get_token_command_access_token — happy paths (JSON mode)
# ---------------------------------------------------------------------------


def _completed(stdout: str, stderr: str = "", returncode: int = 0) -> MagicMock:
    """Build a stand-in for a subprocess.CompletedProcess."""
    proc = MagicMock(spec=subprocess.CompletedProcess)
    proc.stdout = stdout
    proc.stderr = stderr
    proc.returncode = returncode
    return proc


def _creds(**overrides) -> MagicMock:
    creds = MagicMock(spec=FabricSparkCredentials)
    creds.token_command = ["/bin/get-token"]
    creds.token_command_timeout = 30
    creds.token_path = "access_token"
    creds.expires_path = "expires_in"
    for k, v in overrides.items():
        setattr(creds, k, v)
    return creds


class TestTokenCommandJsonMode:
    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_default_paths_match_oauth2_shape(self, mock_run):
        """Default token_path/expires_path should resolve a standard OAuth 2.0 response."""
        mock_run.return_value = _completed(
            json.dumps({"access_token": "tok-123", "expires_in": 1800})
        )
        before = int(time.time())

        token = get_token_command_access_token(_creds())

        assert isinstance(token, AccessToken)
        assert token.token == "tok-123"
        assert before + 1800 <= token.expires_on <= int(time.time()) + 1800
        assert mock_run.call_args.args[0] == ["/bin/get-token"]
        assert mock_run.call_args.kwargs["capture_output"] is True
        assert mock_run.call_args.kwargs["text"] is True

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_camelcase_path_resolves_azure_cli_shape(self, mock_run):
        """Setting token_path='accessToken' resolves Azure CLI's default JSON output."""
        # az account get-access-token --resource X
        mock_run.return_value = _completed(
            json.dumps(
                {
                    "accessToken": "az-tok",
                    "expiresOn": "2099-01-01 00:00:00.000000",
                    "expires_on": 4070908800,  # absolute unix ts > 10^9
                    "subscription": "sub",
                    "tenant": "tenant",
                    "tokenType": "Bearer",
                }
            )
        )
        creds = _creds(token_path="accessToken", expires_path="expires_on")

        token = get_token_command_access_token(creds)

        assert token.token == "az-tok"
        assert token.expires_on == 4070908800

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_dotted_path_resolves_vault_shape(self, mock_run):
        """Vault returns nested structure; dotted path navigates it."""
        mock_run.return_value = _completed(
            json.dumps(
                {
                    "data": {
                        "access_token": "vault-tok",
                        "lease_duration": 1800,
                    }
                }
            )
        )
        creds = _creds(token_path="data.access_token", expires_path="data.lease_duration")

        token = get_token_command_access_token(creds)

        assert token.token == "vault-tok"
        # 1800 < 10^9 -> relative seconds
        assert token.expires_on >= int(time.time()) + 1700

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_string_command_is_shell_split(self, mock_run):
        mock_run.return_value = _completed(
            json.dumps({"access_token": "x", "expires_in": 60})
        )

        creds = _creds(token_command="/bin/get-token --user me")
        get_token_command_access_token(creds)

        assert mock_run.call_args.args[0] == ["/bin/get-token", "--user", "me"]

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_timeout_passed_to_subprocess(self, mock_run):
        mock_run.return_value = _completed(
            json.dumps({"access_token": "x", "expires_in": 60})
        )
        creds = _creds(token_command_timeout=45)

        get_token_command_access_token(creds)
        assert mock_run.call_args.kwargs["timeout"] == 45


# ---------------------------------------------------------------------------
# Raw-text mode — fallback when output isn't JSON or path doesn't resolve
# ---------------------------------------------------------------------------


class TestTokenCommandRawTextMode:
    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_non_json_stdout_used_as_token(self, mock_run):
        """gcloud-style: ``print-access-token`` emits the bare token."""
        mock_run.return_value = _completed("eyJhbGciOiJSUzI1NiIs.payload.sig\n")

        token = get_token_command_access_token(_creds())

        assert token.token == "eyJhbGciOiJSUzI1NiIs.payload.sig"
        # Default 1h fallback
        assert token.expires_on >= int(time.time()) + 3500

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_az_tsv_query_form(self, mock_run):
        """``az ... --query accessToken -o tsv`` emits raw token only."""
        mock_run.return_value = _completed("ey-az-tsv-token\n")
        token = get_token_command_access_token(_creds())
        assert token.token == "ey-az-tsv-token"

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_json_with_unresolved_token_path_falls_back_to_raw(self, mock_run):
        """If output IS JSON but token_path doesn't resolve, the entire
        stdout is treated as the bearer token (the JSON literal in this
        case). The user is responsible for matching the path to the shape.
        """
        mock_run.return_value = _completed(
            json.dumps({"unexpected_key": "hello"})
        )
        token = get_token_command_access_token(_creds())
        # Falls back to raw text — entire stdout
        assert token.token == json.dumps({"unexpected_key": "hello"})

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_empty_output_raises(self, mock_run):
        mock_run.return_value = _completed("")
        with pytest.raises(DbtRuntimeError, match="empty output"):
            get_token_command_access_token(_creds())


# ---------------------------------------------------------------------------
# Expiry interpretation — auto-detect relative vs absolute
# ---------------------------------------------------------------------------


class TestExpiryInterpretation:
    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_small_value_is_relative_seconds(self, mock_run):
        mock_run.return_value = _completed(
            json.dumps({"access_token": "x", "expires_in": 60})
        )
        token = get_token_command_access_token(_creds())
        # 60s relative
        assert int(time.time()) + 50 <= token.expires_on <= int(time.time()) + 60

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_large_value_is_absolute_unix_timestamp(self, mock_run):
        # value > 10^9 unambiguously means unix timestamp (epoch is ~1.76e9 in 2026)
        mock_run.return_value = _completed(
            json.dumps({"access_token": "x", "expires_in": 4070908800})
        )
        token = get_token_command_access_token(_creds())
        assert token.expires_on == 4070908800

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_missing_expiry_defaults_to_one_hour(self, mock_run):
        mock_run.return_value = _completed(json.dumps({"access_token": "x"}))
        token = get_token_command_access_token(_creds())
        assert token.expires_on >= int(time.time()) + 3500

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_malformed_expiry_defaults_to_one_hour(self, mock_run):
        mock_run.return_value = _completed(
            json.dumps({"access_token": "x", "expires_in": "not-a-number"})
        )
        token = get_token_command_access_token(_creds())
        assert token.expires_on >= int(time.time()) + 3500


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestTokenCommandErrors:
    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_missing_token_command_raises(self, mock_run):
        creds = _creds(token_command=None)
        with pytest.raises(DbtRuntimeError, match="requires `token_command`"):
            get_token_command_access_token(creds)
        mock_run.assert_not_called()

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_executable_not_found_raises_clear_error(self, mock_run):
        mock_run.side_effect = FileNotFoundError("no such file")
        with pytest.raises(DbtRuntimeError, match="executable not found"):
            get_token_command_access_token(_creds())

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_command_timeout_raises_clear_error(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd=["/bin/get-token"], timeout=30)
        with pytest.raises(DbtRuntimeError, match="timed out after 30s"):
            get_token_command_access_token(_creds())

    @patch("dbt.adapters.fabricspark.livysession.subprocess.run")
    def test_non_zero_exit_includes_stderr(self, mock_run):
        mock_run.return_value = _completed(
            stdout="",
            stderr="vault: invalid token\n",
            returncode=1,
        )
        with pytest.raises(DbtRuntimeError, match="vault: invalid token"):
            get_token_command_access_token(_creds())


# ---------------------------------------------------------------------------
# get_headers dispatch
# ---------------------------------------------------------------------------


class TestGetHeadersDispatch:
    def setup_method(self):
        # Reset module-level token cache
        import dbt.adapters.fabricspark.livysession as mod

        mod.accessToken = None

    def test_get_headers_dispatches_token_command(self):
        creds = MagicMock(spec=FabricSparkCredentials)
        creds.is_local_mode = False
        creds.authentication = "token_command"

        with patch(
            "dbt.adapters.fabricspark.livysession.get_token_command_access_token",
            return_value=AccessToken(token="cmd-tok", expires_on=9999999999),
        ) as mock_fn:
            headers = get_headers(creds)

        mock_fn.assert_called_once_with(creds)
        assert headers["Authorization"] == "Bearer cmd-tok"
