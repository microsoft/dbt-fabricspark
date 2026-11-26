"""Synchronize dbt descriptions through the OneLake additional table metadata API."""

import re
import time
from copy import deepcopy
from urllib.parse import quote, unquote, urlsplit

import requests
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.fabricspark import livysession
from dbt.adapters.fabricspark._http_utils import parse_retry_after
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.relation import FabricSparkRelation

STORAGE_SCOPE = "https://storage.azure.com/.default"
TRANSIENT = {408, 429, 500, 502, 503, 504}
FABRIC_API_HOST = re.compile(r"([a-z0-9-]*)api\.fabric\.microsoft\.com")


def table_api_base(endpoint: str | None) -> str:
    match = FABRIC_API_HOST.fullmatch(urlsplit(endpoint or "").hostname or "")
    prefix = f"{match[1]}-" if match and match[1] else ""
    return f"https://{prefix}onelake.table.fabric.microsoft.com/common/v1"


def _workspaces_url(endpoint: str | None) -> str:
    parts = urlsplit(endpoint or "")
    try:
        port = parts.port
    except ValueError:
        port = -1
    if (
        parts.scheme != "https"
        or parts.username is not None
        or port not in (None, 443)
        or parts.query
        or parts.fragment
        or not FABRIC_API_HOST.fullmatch(parts.hostname or "")
    ):
        raise DbtRuntimeError(
            "persist_docs_onelake resolves other workspaces and lakehouses only through an "
            "HTTPS Fabric API endpoint such as https://api.fabric.microsoft.com/v1; "
            f"refusing to send a Fabric token to {parts.hostname or 'the configured endpoint'!r}"
        )
    return f"https://{parts.hostname}{parts.path.rstrip('/')}/workspaces"


def _headers(credentials: FabricSparkCredentials, storage: bool) -> dict[str, str]:
    mode = (credentials.authentication or "").lower()
    scope = STORAGE_SCOPE if storage else livysession.AZURE_CREDENTIAL_SCOPE
    providers = {
        "cli": livysession.get_cli_access_token,
        "spn": livysession.get_sp_access_token,
        "token_credential": livysession.get_token_credential_access_token,
        "fabric_notebook": livysession.get_fabric_notebook_access_token,
    }
    if mode not in providers:
        raise DbtRuntimeError(
            "persist_docs_onelake requires CLI, SPN, fabric_notebook or token_credential "
            "authentication to acquire a storage token; a raw Fabric token cannot be reused."
        )
    if mode == "fabric_notebook":
        scope = "storage" if storage else livysession.FABRIC_NOTEBOOK_CREDENTIAL_SCOPE
    token = providers[mode](credentials, scope)
    return {"Authorization": f"Bearer {token.token}", "Accept": "application/json"}


def _wait(response, attempt):
    hint = parse_retry_after(response) if response is not None else 0
    time.sleep(min(30, max(2 ** min(attempt, 5), hint)))


def _read(url, headers, credentials, params=None, *, newly_built=False):
    for attempt in range(max(0, credentials.connect_retries) + 1):
        response = None
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=credentials.http_timeout
            )
        except (requests.Timeout, requests.ConnectionError) as exc:
            cause = type(exc).__name__
        else:
            if response.status_code == 200:
                try:
                    payload = response.json()
                except ValueError:
                    raise DbtRuntimeError(f"OneLake GET {url}: invalid JSON") from None
                if not isinstance(payload, dict):
                    raise DbtRuntimeError(f"OneLake GET {url}: expected a JSON object")
                return payload
            cause = f"HTTP {response.status_code}"
            if response.status_code not in TRANSIENT and not (
                newly_built and response.status_code == 404
            ):
                break
        if attempt < credentials.connect_retries:
            _wait(response, attempt)
    raise DbtRuntimeError(f"OneLake GET {url} failed: {cause}")


def _resolve_id(credentials, url, name, headers):
    matches = set()
    tokens = set()
    params = {}
    while True:
        page = _read(url, headers, credentials, params)
        items = page.get("value")
        if not isinstance(items, list):
            raise DbtRuntimeError(f"Invalid Fabric listing from {url}: expected value array")
        for item in items:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("displayName"), str)
                or not isinstance(item.get("id"), str)
            ):
                raise DbtRuntimeError(f"Invalid item in Fabric listing from {url}")
            if item["displayName"].casefold() == name.casefold():
                matches.add(item["id"])
        token = page.get("continuationToken")
        if not token:
            break
        if not isinstance(token, str) or token in tokens:
            raise DbtRuntimeError(f"Invalid continuation token from {url}")
        tokens.add(token)
        # Fabric returns the token already URL-encoded; requests encodes it again.
        params = {"continuationToken": unquote(token)}
    if len(matches) != 1:
        raise DbtRuntimeError(
            f"Expected one Fabric item named {name!r} in {url}; found {len(matches)}"
        )
    return matches.pop()


def metadata_url(credentials: FabricSparkCredentials, relation: FabricSparkRelation) -> str:
    workspace_id = credentials.workspaceid
    if not workspace_id or not credentials.lakehouseid or not credentials.lakehouse:
        raise DbtRuntimeError(
            "persist_docs_onelake requires Fabric workspaceid, lakehouseid and lakehouse"
        )
    qualified = relation.include_policy.database
    lakehouse = relation.database if qualified else relation.schema
    schema = relation.schema if qualified else "dbo"
    if not lakehouse or not schema or not relation.identifier:
        raise DbtRuntimeError(f"Incomplete OneLake target relation: {relation}")
    item_id = credentials.lakehouseid
    other_workspace = bool(qualified and relation.workspace)
    other_lakehouse = lakehouse.casefold() != credentials.lakehouse.casefold()
    if other_workspace or other_lakehouse:
        workspaces = _workspaces_url(credentials.endpoint)
        fabric_headers = _headers(credentials, storage=False)
        if other_workspace:
            workspace_id = _resolve_id(credentials, workspaces, relation.workspace, fabric_headers)
        if other_lakehouse or workspace_id != credentials.workspaceid:
            item_id = _resolve_id(
                credentials,
                f"{workspaces}/{quote(workspace_id, safe='')}/lakehouses",
                lakehouse,
                fabric_headers,
            )
    return (
        f"{table_api_base(credentials.endpoint)}/{quote(workspace_id, safe='')}/{quote(item_id, safe='')}"
        f"/schemas/{quote(schema, safe='')}/tables/{quote(relation.identifier, safe='')}/additionalMetadata"
    )


def _merge(current, description, columns):
    merged = deepcopy(current)
    entries = merged.get("columns", [])
    if not isinstance(entries, list):
        raise DbtRuntimeError("Invalid OneLake metadata: expected columns array")
    existing = {}
    for entry in [merged, *entries]:
        if (
            not isinstance(entry, dict)
            or ("tags" in entry and not isinstance(entry["tags"], dict))
            or ("description" in entry and not isinstance(entry["description"], str))
        ):
            raise DbtRuntimeError("Invalid OneLake description or tags")
    for entry in entries:
        name = entry.get("columnName")
        if not isinstance(name, str) or not name or name in existing:
            raise DbtRuntimeError("Invalid or duplicate OneLake columnName")
        existing[name] = entry
    if description is not None:
        merged["description"] = description
    for name, value in columns.items():
        if name not in existing:
            existing[name] = {"columnName": name}
            merged.setdefault("columns", []).append(existing[name])
        existing[name]["description"] = value
    return merged


def persist_descriptions(
    credentials, relation, description: str | None, columns: dict[str, str]
) -> None:
    if description is None and not columns:
        return
    for field, value in [("table", description), *columns.items()]:
        if value is not None and len(value) > 1024:
            raise DbtRuntimeError(
                f"OneLake description for {relation} ({field}) exceeds 1024 characters"
            )
    headers = _headers(credentials, storage=True)
    url = metadata_url(credentials, relation)
    current = _read(url, headers, credentials, newly_built=True)
    merged = _merge(current, description, columns)
    if merged == current:
        return
    # Fabric returns columnId on GET but rejects it alongside columnName on POST.
    for column in merged.get("columns", []):
        column.pop("columnId", None)
    # POST replaces all metadata and has no idempotency key, so it is never replayed.
    try:
        response = requests.post(
            url, headers=headers, json=merged, timeout=credentials.http_timeout
        )
    except (requests.Timeout, requests.ConnectionError) as exc:
        cause = type(exc).__name__
    else:
        if response.status_code == 200:
            stored = _read(url, headers, credentials)
            if _merge(stored, description, columns) != stored:
                raise DbtRuntimeError(
                    f"OneLake did not retain the selected descriptions for {relation}"
                )
            return
        cause = f"HTTP {response.status_code}"
        if response.status_code != 408 and response.status_code < 500:
            raise DbtRuntimeError(
                f"OneLake rejected the metadata write to {url}: {cause}. "
                "The dbt build is not rolled back."
            )
    raise DbtRuntimeError(
        f"OneLake metadata write to {url} has an unknown outcome ({cause}) and may still "
        "complete; it was not retried. The dbt build is not rolled back."
    )
