import json
import os
import time
import uuid

import pytest
import requests
from dotenv import load_dotenv

from dbt.adapters.fabricspark.shortcut import Shortcut, TargetName
from dbt.adapters.fabricspark.shortcuts import ShortcutClient, create_and_prune_shortcuts
from tests.functional.fabric_client import AzureCliTokenProvider, StaticTokenProvider


@pytest.fixture(scope="class", autouse=True)
def _prefix_aware_run_sql():
    """Keep this REST-only test from provisioning the dbt project or opening Livy."""
    yield


def test_fabric_shortcut_idempotence_failures_and_pruning(monkeypatch):
    load_dotenv("test.env", override=False)
    workspace_id = os.environ["WORKSPACE_ID_1"]
    endpoint = os.environ.get("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1").rstrip("/")
    configured_token = os.environ.get("FABRIC_INTEGRATION_TESTS_TOKEN")
    token_provider = (
        StaticTokenProvider(configured_token) if configured_token else AzureCliTokenProvider()
    )
    token = token_provider.get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    prefix = f"dbt_shortcuts_{uuid.uuid4().hex[:12]}"

    def request(method, path, **kwargs):
        return requests.request(
            method,
            f"{endpoint}/workspaces/{workspace_id}/{path}",
            headers=headers,
            timeout=30,
            **kwargs,
        )

    def create_lakehouse(name):
        response = request(
            "POST",
            "lakehouses",
            json={"displayName": name, "description": "Temporary shortcut regression test"},
        )
        response.raise_for_status()
        item_id = response.json().get("id")
        assert item_id, "Fabric did not return a lakehouse ID"
        return item_id

    def shortcut(name, item_id, path="Files", source_item_id=None):
        return Shortcut(
            path=path,
            shortcut_name=name,
            target=TargetName.onelake,
            source_path="Files",
            source_workspace_id=workspace_id,
            source_item_id=source_item_id or item_id,
        )

    def shortcut_config(*items):
        return json.dumps(
            {
                "shortcuts": [
                    {
                        "path": item.path,
                        "shortcut_name": item.shortcut_name,
                        "target": "onelake",
                        "source_path": item.source_path,
                        "source_workspace_id": item.source_workspace_id,
                        "source_item_id": item.source_item_id,
                    }
                    for item in items
                ]
            }
        )

    def read_shortcut(item_id, path, name, expected_status):
        deadline = time.monotonic() + 20
        status = None
        while time.monotonic() < deadline:
            response = request("GET", f"items/{item_id}/shortcuts/{path}/{name}")
            status = response.status_code
            if status == expected_status:
                return response
            if status not in (200, 404):
                response.raise_for_status()
            time.sleep(0.5)
        raise AssertionError(
            f"Shortcut did not reach HTTP {expected_status}; last status was {status}"
        )

    destination_id = source_id = None
    try:
        destination_id = create_lakehouse(f"{prefix}_destination")
        source_id = create_lakehouse(f"{prefix}_source")
        client = ShortcutClient(token, workspace_id, destination_id, endpoint)

        keep = shortcut(f"keep_{prefix}", source_id)
        config = shortcut_config(keep)
        client.create_shortcuts(config)
        created = read_shortcut(destination_id, keep.path, keep.shortcut_name, 200).json()
        assert created["target"]["type"] == "OneLake"
        assert "oneLake" in created["target"]

        posts, deletes = [], []
        original_post, original_delete = requests.post, requests.delete

        def track_post(*args, **kwargs):
            posts.append(None)
            return original_post(*args, **kwargs)

        def track_delete(*args, **kwargs):
            deletes.append(None)
            return original_delete(*args, **kwargs)

        with monkeypatch.context() as context:
            context.setattr(requests, "post", track_post)
            context.setattr(requests, "delete", track_delete)
            client.create_shortcuts(config)
        assert not deletes
        assert not posts
        assert read_shortcut(destination_id, keep.path, keep.shortcut_name, 200).json()["target"]

        bad_name = f"bad_{prefix}"
        bad_config = shortcut_config(
            shortcut(bad_name, source_id, source_item_id="00000000-0000-0000-0000-000000000000")
        )
        with pytest.raises(ValueError) as failure:
            client.create_shortcuts(bad_config)
        assert bad_name in str(failure.value)
        assert any(f"HTTP {status}" in str(failure.value) for status in (400, 403, 404))

        orphan = shortcut(f"orphan_{prefix}", source_id)
        outside = shortcut(f"outside_{prefix}", source_id, path="Tables")
        client.create_shortcut(orphan)
        client.create_shortcut(outside)
        read_shortcut(destination_id, orphan.path, orphan.shortcut_name, 200)
        read_shortcut(destination_id, outside.path, outside.shortcut_name, 200)

        create_and_prune_shortcuts(client, config, prune=True)
        read_shortcut(destination_id, keep.path, keep.shortcut_name, 200)
        read_shortcut(destination_id, outside.path, outside.shortcut_name, 200)
        read_shortcut(destination_id, orphan.path, orphan.shortcut_name, 404)
    finally:
        for item_id in (destination_id, source_id):
            if item_id:
                response = request("DELETE", f"lakehouses/{item_id}")
                if response.status_code not in (200, 204):
                    response.raise_for_status()
                    raise AssertionError("Fabric did not delete a temporary lakehouse")
