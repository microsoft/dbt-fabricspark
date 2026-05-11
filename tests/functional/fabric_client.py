from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("fabric_client")

AZURE_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"


@dataclass(frozen=True)
class Lakehouse:
    """Represents a provisioned Fabric Lakehouse."""

    id: str
    name: str
    schemas_enabled: bool


@dataclass(frozen=True)
class Environment:
    """Represents a provisioned Fabric Environment."""

    id: str
    name: str


class TokenProvider(ABC):
    """Abstract base for obtaining Fabric access tokens."""

    @abstractmethod
    def get_token(self) -> str:
        """Return a valid bearer token string."""


class AzureCliTokenProvider(TokenProvider):
    """Obtains tokens via ``az login`` (AzureCliCredential)."""

    def get_token(self) -> str:
        from azure.identity import AzureCliCredential

        credential = AzureCliCredential(process_timeout=30)
        return credential.get_token(AZURE_CREDENTIAL_SCOPE).token


class StaticTokenProvider(TokenProvider):
    """Uses a pre-fetched token string (e.g. from CI OIDC flow)."""

    def __init__(self, token: str) -> None:
        self._token = token

    def get_token(self) -> str:
        return self._token


class FabricClient:
    """Client for the Microsoft Fabric REST API (v1).

    Parameters
    ----------
    workspace_id : str
        Target Fabric workspace UUID.
    api_endpoint : str
        Base URL for the Fabric API (e.g. ``https://api.fabric.microsoft.com/v1``).
    token_provider : TokenProvider
        Strategy for obtaining bearer tokens.
    """

    def __init__(
        self,
        workspace_id: str,
        api_endpoint: str,
        token_provider: TokenProvider,
    ) -> None:
        self.workspace_id = workspace_id
        self.api_endpoint = api_endpoint.rstrip("/")
        self._token_provider = token_provider

        self._session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "DELETE"],
        )
        self._session.mount("https://", HTTPAdapter(max_retries=retries))

    def _headers(self) -> dict[str, str]:
        token = self._token_provider.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        return f"{self.api_endpoint}/workspaces/{self.workspace_id}/{path}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        expected_status: tuple[int, ...] = (200, 201),
        timeout: int = 120,
    ) -> requests.Response:
        url = self._url(path)
        logger.info("%s %s", method.upper(), url)
        resp = self._session.request(
            method, url, json=json, headers=self._headers(), timeout=timeout
        )
        if resp.status_code not in expected_status:
            logger.error(
                "Fabric API error %d on %s %s: %s",
                resp.status_code,
                method.upper(),
                url,
                resp.text,
            )
            resp.raise_for_status()
        return resp

    def create_lakehouse(
        self,
        name: str,
        enable_schemas: bool = False,
    ) -> Lakehouse:
        """Create a Fabric Lakehouse and return its metadata.

        Parameters
        ----------
        name : str
            Display name for the lakehouse.
        enable_schemas : bool
            When True, creates a schema-enabled lakehouse.
        """
        body: dict[str, Any] = {
            "displayName": name,
            "description": f"Auto-provisioned test lakehouse: {name}",
        }
        if enable_schemas:
            body["creationPayload"] = {"enableSchemas": True}

        resp = self._request("POST", "lakehouses", json=body, expected_status=(201,))
        data = resp.json()
        lh = Lakehouse(id=data["id"], name=name, schemas_enabled=enable_schemas)
        logger.info("Created lakehouse %s (id=%s, schemas=%s)", lh.name, lh.id, lh.schemas_enabled)
        return lh

    def delete_lakehouse(self, lakehouse_id: str) -> None:
        """Delete a Fabric Lakehouse (best-effort, logs warnings on failure)."""
        try:
            self._request(
                "DELETE",
                f"lakehouses/{lakehouse_id}",
                expected_status=(200, 204),
            )
            logger.info("Deleted lakehouse %s", lakehouse_id)
        except Exception:
            logger.warning("Failed to delete lakehouse %s", lakehouse_id, exc_info=True)

    def create_environment(self, name: str) -> Environment:
        """Create a Fabric Environment."""
        resp = self._request(
            "POST",
            "environments",
            json={"displayName": name, "description": "Auto-provisioned test environment"},
            expected_status=(200, 201),
        )
        data = resp.json()
        env = Environment(id=data["id"], name=name)
        logger.info("Created environment %s (id=%s)", env.name, env.id)
        return env

    def configure_environment(
        self,
        environment_id: str,
        *,
        pool_name: str = "Starter Pool",
        pool_type: str = "Workspace",
        driver_cores: int = 8,
        driver_memory: str = "56g",
        executor_cores: int = 8,
        executor_memory: str = "56g",
        min_executors: int = 1,
        max_executors: int = 1,
        runtime_version: str = "1.3",
    ) -> None:
        """Configure Spark compute for a staged environment."""
        self._request(
            "PATCH",
            f"environments/{environment_id}/staging/sparkcompute",
            json={
                "instancePool": {"name": pool_name, "type": pool_type},
                "driverCores": driver_cores,
                "driverMemory": driver_memory,
                "executorCores": executor_cores,
                "executorMemory": executor_memory,
                "dynamicExecutorAllocation": {
                    "enabled": False,
                    "minExecutors": min_executors,
                    "maxExecutors": max_executors,
                },
                "runtimeVersion": runtime_version,
            },
            expected_status=(200, 201),
        )
        logger.info("Configured environment %s with %s", environment_id, pool_name)

    def publish_environment(
        self,
        environment_id: str,
        timeout: int = 300,
        poll_interval: int = 10,
    ) -> None:
        """Publish staged environment changes and wait for completion."""
        self._request(
            "POST",
            f"environments/{environment_id}/staging/publish",
            expected_status=(200, 202),
        )

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            time.sleep(poll_interval)
            resp = self._request(
                "GET",
                f"environments/{environment_id}",
                expected_status=(200,),
            )
            state = (
                resp.json().get("properties", {}).get("publishDetails", {}).get("state", "unknown")
            )
            logger.info("Environment %s publish state: %s", environment_id, state)
            if state.lower() in ("success", "completed"):
                return

        logger.warning(
            "Environment %s publish did not complete within %ds", environment_id, timeout
        )

    def delete_environment(self, environment_id: str) -> None:
        """Delete a Fabric Environment (best-effort)."""
        try:
            self._request(
                "DELETE",
                f"environments/{environment_id}",
                expected_status=(200, 204),
            )
            logger.info("Deleted environment %s", environment_id)
        except Exception:
            logger.warning("Failed to delete environment %s", environment_id, exc_info=True)

    # Terminal Livy session states — sessions in these states no longer
    # consume a session slot and should not be counted as "active".
    _LIVY_TERMINAL_STATES = frozenset({"dead", "killed", "error", "shutting_down", "success"})

    def list_livy_sessions(
        self,
        lakehouse_id: str,
        *,
        livy_api_version: str = "2023-12-01",
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        """Return Livy sessions for a lakehouse via the Fabric REST API.

        Calls ``GET /workspaces/{ws}/lakehouses/{lh}/livyApi/versions/{v}/sessions``.

        Parameters
        ----------
        lakehouse_id : str
            Target Lakehouse UUID.
        livy_api_version : str
            Livy API version segment of the URL. Defaults to the version the
            adapter itself uses (see ``FabricSparkCredentials.lakehouse_endpoint``).
        active_only : bool
            When True (default), filters out sessions in terminal states
            (``dead``, ``killed``, ``error``, ``shutting_down``, ``success``).
        """
        path = f"lakehouses/{lakehouse_id}/livyApi/versions/{livy_api_version}/sessions"
        resp = self._request("GET", path, expected_status=(200,))
        sessions = resp.json().get("sessions", []) or []
        if not active_only:
            return sessions
        return [
            s
            for s in sessions
            if str(s.get("state", "")).lower() not in self._LIVY_TERMINAL_STATES
        ]
