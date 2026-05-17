from __future__ import annotations

import datetime as dt
import importlib
import json
import os
import re
import threading
import time
from typing import Any, Optional

import requests
from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import AzureCliCredential, ClientSecretCredential
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials

logger = AdapterLogger("Microsoft Fabric-Spark")

livysession_credentials: FabricSparkCredentials

DEFAULT_POLL_WAIT = 10
DEFAULT_POLL_STATEMENT_WAIT = 5
AZURE_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
FABRIC_NOTEBOOK_CREDENTIAL_SCOPE = "pbi"
accessToken: AccessToken = None

# Global lock to ensure thread-safe token refresh
_token_lock = threading.Lock()

# Process-level cache for lakehouse properties (avoids repeated API calls per connection open)
_lakehouse_props_cache: dict[tuple[str, str, str], dict] = {}
_lakehouse_props_lock = threading.Lock()

# Process-level cache for user-supplied TokenCredential instances, keyed by
# the (dotted_path, repr-of-sorted-kwargs) tuple. Lets refresh-on-expiry
# reuse the same instance without re-importing on every header build. Using
# repr() of the sorted kwargs makes the key stable even when kwargs contain
# unhashable values (nested dicts, lists from YAML).
_custom_credential_cache: dict[tuple[str, str], Any] = {}
_custom_credential_lock = threading.Lock()

# Dotted-path identifier validation (defence-in-depth — importlib won't
# execute shell, but rejecting non-identifier chars gives clearer errors).
_DOTTED_PATH_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$")


def read_session_id_from_file(file_path: str) -> Optional[str]:
    """Read session ID from file if it exists and contains a valid ID.

    Parameters
    ----------
    file_path : str
        Path to the session ID file.

    Returns
    -------
    Optional[str]
        The session ID if file exists and contains one, None otherwise.
    """
    try:
        if not os.path.exists(file_path):
            logger.debug(f"Session ID file does not exist: {file_path}")
            return None

        with open(file_path, "r") as f:
            session_id = f.read().strip()
            if session_id:
                logger.debug(f"Read session ID from file: {session_id}")
                return session_id
            else:
                logger.debug(f"Session ID file exists but is empty: {file_path}")
                return None
    except Exception as ex:
        logger.debug(f"Error reading session ID file: {ex}")
        return None


def write_session_id_to_file(file_path: str, session_id: str) -> bool:
    """Write session ID to file.

    Parameters
    ----------
    file_path : str
        Path to the session ID file.
    session_id : str
        The session ID to write.

    Returns
    -------
    bool
        True if successful, False otherwise.
    """
    try:
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)

        with open(file_path, "w") as f:
            f.write(session_id)
        logger.debug(f"Wrote session ID to file: {session_id} -> {file_path}")
        return True
    except Exception as ex:
        logger.warning(f"Error writing session ID to file: {ex}")
        return False


def is_token_refresh_necessary(unixTimestamp: int) -> bool:
    # Convert to datetime object
    dt_object = dt.datetime.fromtimestamp(unixTimestamp)
    # Convert to local time
    local_time = time.localtime(time.time())

    # Calculate difference
    difference = dt_object - dt.datetime.fromtimestamp(time.mktime(local_time))
    if int(difference.total_seconds() / 60) < 5:
        logger.debug(f"Token Refresh necessary in {int(difference.total_seconds() / 60)}")
        return True
    else:
        return False


def get_cli_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the CLI credentials

    First login with:

    ```bash
    az login
    ```

    Parameters
    ----------
    credentials: FabricConnectionManager
        The credentials.

    Returns
    -------
    out : AccessToken
        Access token.
    """
    _ = credentials
    accessToken = AzureCliCredential().get_token(AZURE_CREDENTIAL_SCOPE)
    return accessToken


def get_sp_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the SP credentials.

    Parameters
    ----------
    credentials : FabricCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    accessToken = ClientSecretCredential(
        str(credentials.tenant_id), str(credentials.client_id), str(credentials.client_secret)
    ).get_token(AZURE_CREDENTIAL_SCOPE)
    return accessToken


def get_default_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the SP Default Credentials.

    Parameters
    ----------
    credentials : FabricCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    expires_on = 1845972874

    # Create an AccessToken instance
    accessToken = AccessToken(token=credentials.accessToken, expires_on=expires_on)
    return accessToken


def _load_custom_credential(credentials: FabricSparkCredentials) -> Any:
    """
    Import and instantiate the user-supplied TokenCredential.

    The instance is cached per (dotted_path, kwargs) tuple so that refresh
    cycles reuse the same object (matching how azure-identity credentials
    are typically held).

    Parameters
    ----------
    credentials : FabricSparkCredentials
        Credentials carrying ``credential_class`` (dotted path) and
        ``credential_kwargs``.

    Returns
    -------
    out : Any
        An instance of the user-supplied TokenCredential implementation.
    """
    dotted = credentials.credential_class
    if not dotted:
        raise DbtRuntimeError("authentication='token_credential' requires `credential_class`.")
    if not _DOTTED_PATH_PATTERN.match(dotted):
        raise DbtRuntimeError(
            f"credential_class must be a dotted path like 'pkg.module.ClassName', got: {dotted!r}"
        )
    kwargs = credentials.credential_kwargs or {}
    # repr() of sorted items keeps the cache key hashable even when kwargs
    # contain nested dicts/lists from YAML.
    cache_key = (dotted, repr(sorted(kwargs.items(), key=lambda kv: kv[0])))

    with _custom_credential_lock:
        if cache_key in _custom_credential_cache:
            return _custom_credential_cache[cache_key]

        module_path, _, class_name = dotted.rpartition(".")
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise DbtRuntimeError(
                f"Could not import module for credential_class={dotted!r}: {exc}"
            ) from exc
        try:
            cls = getattr(module, class_name)
        except AttributeError as exc:
            raise DbtRuntimeError(
                f"Module {module_path!r} has no attribute {class_name!r} "
                f"(from credential_class={dotted!r})"
            ) from exc

        try:
            instance = cls(**kwargs)
        except TypeError as exc:
            raise DbtRuntimeError(
                f"Failed to instantiate {dotted!r} with credential_kwargs: {exc}"
            ) from exc

        # TokenCredential is a @runtime_checkable Protocol — isinstance does
        # the structural get_token check, so any class exposing get_token
        # passes whether or not it inherits from TokenCredential.
        if not isinstance(instance, TokenCredential):
            raise DbtRuntimeError(
                f"{dotted!r} must implement azure.core.credentials.TokenCredential "
                f"(missing callable get_token)."
            )

        _custom_credential_cache[cache_key] = instance
        return instance


def get_token_credential_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token from a user-supplied TokenCredential class.

    The class is loaded by dotted path from ``credentials.credential_class``
    and instantiated with ``credentials.credential_kwargs``.

    Parameters
    ----------
    credentials : FabricSparkCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token returned by the user-supplied credential.
    """
    credential = _load_custom_credential(credentials)
    result = credential.get_token(AZURE_CREDENTIAL_SCOPE)
    if not isinstance(result, AccessToken):
        raise DbtRuntimeError(
            f"{credentials.credential_class!r}.get_token() must return an "
            f"azure.core.credentials.AccessToken, got {type(result).__name__}."
        )
    return result


def get_fabric_notebook_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using notebookutils.

    Works in both Fabric PySpark and Python notebooks.

    Note: notebookutils is only available in Fabric notebook runtime environments.
    It is not installable via pip and will not resolve in local development.

    Parameters
    ----------
    credentials : FabricSparkCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    import base64  # noqa: F401

    import notebookutils  # type: ignore  # noqa: F401 - only available in Fabric runtime

    _ = credentials
    aad_token = notebookutils.credentials.getToken(FABRIC_NOTEBOOK_CREDENTIAL_SCOPE)
    expires_on = json.loads(base64.b64decode(aad_token.split(".")[1] + "=="))["exp"]

    now = time.time()
    remaining_seconds = expires_on - now
    remaining_minutes = remaining_seconds / 60
    logger.debug(
        f"Token expiry: {dt.datetime.fromtimestamp(expires_on).isoformat()}, "
        f"Current time: {dt.datetime.fromtimestamp(now).isoformat()}, "
        f"Remaining: {remaining_minutes:.1f} minutes"
    )

    accessToken = AccessToken(token=aad_token, expires_on=expires_on)
    return accessToken


def get_headers(credentials: FabricSparkCredentials, tokenPrint: bool = False) -> dict[str, str]:
    """Get HTTP headers for Livy requests.

    For local mode, no authentication is required.
    For Fabric mode, Azure authentication is used.
    """
    if credentials.is_local_mode:
        # Local Livy doesn't require authentication
        return {"Content-Type": "application/json"}

    global accessToken
    with _token_lock:
        if accessToken is None or is_token_refresh_necessary(accessToken.expires_on):
            if credentials.authentication and credentials.authentication.lower() == "cli":
                logger.info("Using CLI auth")
                accessToken = get_cli_access_token(credentials)
            elif credentials.authentication and credentials.authentication.lower() == "int_tests":
                logger.info("Using int_tests auth")
                accessToken = get_default_access_token(credentials)
            elif (
                credentials.authentication
                and credentials.authentication.lower() == "fabric_notebook"
            ):
                logger.info("Using Fabric Notebook auth")
                accessToken = get_fabric_notebook_access_token(credentials)
            elif (
                credentials.authentication
                and credentials.authentication.lower() == "token_credential"
            ):
                logger.info(f"Using token_credential auth ({credentials.credential_class})")
                accessToken = get_token_credential_access_token(credentials)
            else:
                logger.info("Using SPN auth")
                accessToken = get_sp_access_token(credentials)

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {accessToken.token}"}
    if tokenPrint:
        logger.debug(f"token is : {accessToken.token}")

    return headers


def _parse_retry_after(response: requests.Response) -> float:
    """Extract wait time from Retry-After header or 429 response body.

    Falls back to 0 if no hint is found.
    """
    header = response.headers.get("Retry-After", "")
    if header:
        try:
            return float(header)
        except ValueError:
            pass
    # Fabric 429 body sometimes includes a retry-after timestamp in the message
    try:
        body = response.json()
        msg = body.get("message", "")
        # Fabric 429 body includes a timestamp like "...until: 4/17/2026 12:22:35 PM (UTC)"
        if "until:" in msg:
            ts_str = msg.split("until:")[1].strip().rstrip(")")
            ts_str = ts_str.replace("(UTC", "").strip()
            target = dt.datetime.strptime(ts_str, "%m/%d/%Y %I:%M:%S %p")
            delta = (target - dt.datetime.utcnow()).total_seconds()
            return max(delta, 0)
    except Exception:
        pass
    return 0


def get_lakehouse_properties(credentials: FabricSparkCredentials) -> dict:
    """Fetch lakehouse properties from the Fabric REST API.

    Calls GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId} and returns
    the ``properties`` dict from the response. The presence of ``defaultSchema``
    in the returned dict indicates a schema-enabled lakehouse.

    Results are cached per process so parallel calls don't stampede
    the API on every connection open.

    Returns an empty dict for local mode (no Fabric API available).
    """
    if credentials.is_local_mode:
        return {}

    cache_key = (credentials.endpoint, credentials.workspaceid, credentials.lakehouseid)

    with _lakehouse_props_lock:
        if cache_key in _lakehouse_props_cache:
            logger.debug("Lakehouse properties served from cache")
            return _lakehouse_props_cache[cache_key]

    headers = get_headers(credentials)
    url = f"{credentials.endpoint}/workspaces/{credentials.workspaceid}/lakehouses/{credentials.lakehouseid}"

    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 429:
                retry_after = _parse_retry_after(response)
                wait = max(retry_after, 2**attempt * 2)  # at least 2, 4, 8, 16, 32s
                logger.debug(
                    f"Lakehouse properties API returned 429, "
                    f"retrying in {wait:.0f}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait)
                continue
            response.raise_for_status()
            properties = response.json().get("properties", {})
            logger.debug(f"Lakehouse properties: {properties}")

            with _lakehouse_props_lock:
                _lakehouse_props_cache[cache_key] = properties

            return properties
        except requests.exceptions.HTTPError:
            if attempt < max_retries - 1:
                wait = 2**attempt * 2
                logger.debug(
                    f"Lakehouse properties API failed, "
                    f"retrying in {wait}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait)
                continue
            logger.warning(
                f"Failed to fetch lakehouse properties after {max_retries} attempts, "
                f"defaulting to empty"
            )
            return {}
        except Exception as e:
            logger.warning(f"Failed to fetch lakehouse properties, defaulting to empty: {e}")
            return {}

    logger.warning(
        f"Failed to fetch lakehouse properties after {max_retries} retries (429), "
        f"defaulting to empty"
    )
    return {}


from dbt.adapters.fabricspark.singleton_livy import (  # noqa: E402
    LivyConnection,
    LivyCursor,
    LivySession,
    LivySessionConnectionWrapper,
    LivySessionManager,
)

__all__ = [
    "LivyConnection",
    "LivyCursor",
    "LivySession",
    "LivySessionConnectionWrapper",
    "LivySessionManager",
    "get_cli_access_token",
    "get_default_access_token",
    "get_fabric_notebook_access_token",
    "get_headers",
    "get_lakehouse_properties",
    "get_sp_access_token",
    "get_token_credential_access_token",
    "is_token_refresh_necessary",
    "read_session_id_from_file",
    "write_session_id_to_file",
]
