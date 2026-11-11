import json
from unittest.mock import Mock, mock_open, patch

import pytest
import requests

from dbt.adapters.fabricspark import shortcuts
from dbt.adapters.fabricspark.shortcut import Shortcut, TargetName
from dbt.adapters.fabricspark.shortcuts import ShortcutClient

WORKSPACE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ITEM_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def _shortcut(name="name", path="Files", source_path="source_path"):
    return Shortcut(
        path=path,
        shortcut_name=name,
        target=TargetName.onelake,
        source_path=source_path,
        source_workspace_id=WORKSPACE_ID,
        source_item_id=ITEM_ID,
    )


def _response(status, body=None, text=""):
    response = Mock(status_code=status, text=text)
    response.json.return_value = body or {}
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(
            f"HTTP {status}", response=response
        )
    return response


def _config(*shortcuts):
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
                for item in shortcuts
            ]
        }
    )


def _client():
    return ShortcutClient("token", WORKSPACE_ID, "destination-id", retry_wait_seconds=0)


def test_fabric_response_with_real_one_lake_shape_is_unchanged():
    shortcut = _shortcut()
    client = _client()
    response_target = {
        "type": "OneLake",
        "oneLake": {
            "workspaceId": WORKSPACE_ID.upper(),
            "itemId": ITEM_ID.upper(),
            "path": "/source_path/",
            "extraResponseField": "ignored",
        },
        "extraTargetField": "ignored",
    }
    with (
        patch(
            "requests.get",
            return_value=_response(
                200, {"path": "Files", "name": "name", "target": response_target}
            ),
        ) as get,
        patch("requests.delete") as delete,
        patch("requests.post") as post,
        patch("dbt.adapters.fabricspark.shortcuts.time.sleep") as sleep,
    ):
        client.create_shortcut(shortcut)

    get.assert_called_once()
    delete.assert_not_called()
    post.assert_not_called()
    sleep.assert_not_called()
    assert shortcut.get_target_body() == {
        "type": "OneLake",
        "oneLake": {
            "workspaceId": WORKSPACE_ID,
            "itemId": ITEM_ID,
            "path": "source_path",
        },
    }


def test_changed_target_is_deleted_then_recreated():
    client = _client()
    shortcut = _shortcut()
    calls = []
    with (
        patch(
            "requests.get",
            return_value=_response(
                200,
                {
                    "path": "Files",
                    "name": "name",
                    "target": {
                        "type": "OneLake",
                        "oneLake": {
                            "workspaceId": WORKSPACE_ID,
                            "itemId": ITEM_ID,
                            "path": "different_path",
                        },
                    },
                },
            ),
        ),
        patch(
            "requests.delete",
            side_effect=lambda *a, **k: (calls.append("delete"), _response(204))[1],
        ),
        patch(
            "requests.post", side_effect=lambda *a, **k: (calls.append("post"), _response(201))[1]
        ),
        patch("dbt.adapters.fabricspark.shortcuts.time.sleep"),
    ):
        client.create_shortcut(shortcut)

    assert calls == ["delete", "post"]


def test_retries_are_per_shortcut_and_report_actual_attempts_and_cause():
    client = _client()
    first, second = _shortcut("first"), _shortcut("second")
    with (
        patch("requests.get", return_value=_response(404)),
        patch(
            "requests.post",
            side_effect=[
                _response(500, text="temporary failure"),
                _response(201),
                _response(500, text="server failure"),
                _response(500, text="server failure"),
                _response(500, text="server failure"),
            ],
        ) as post,
        patch("dbt.adapters.fabricspark.shortcuts.time.sleep") as sleep,
    ):
        with pytest.raises(ValueError) as exc:
            client.create_shortcuts(_config(first, second))

    assert post.call_count == 5
    assert sleep.call_count == 3
    assert "second" in str(exc.value)
    assert "after 3 attempts" in str(exc.value)
    assert "HTTP 500" in str(exc.value)
    assert "server failure" in str(exc.value)


def test_deterministic_bad_request_is_not_retried():
    client = _client()
    shortcut = _shortcut("invalid")
    with (
        patch("requests.get", return_value=_response(404)),
        patch("requests.post", return_value=_response(400, text="invalid source item")) as post,
        patch("dbt.adapters.fabricspark.shortcuts.time.sleep") as sleep,
    ):
        with pytest.raises(ValueError) as exc:
            client.create_shortcuts(_config(shortcut))

    post.assert_called_once()
    sleep.assert_not_called()
    assert "invalid" in str(exc.value)
    assert "HTTP 400" in str(exc.value)
    assert "invalid source item" in str(exc.value)


def test_prune_pages_and_deletes_only_unlisted_direct_children():
    client = _client()
    configured = _shortcut("keep", path="Files/managed")
    page1 = _response(
        200,
        {
            "value": [
                {"path": "Files/managed", "name": "remove-one", "target": {"type": "OneLake"}},
                {"path": "Files/managed", "name": "keep", "target": {"type": "OneLake"}},
                {"path": "Files/managed/nested", "name": "nested", "target": {"type": "OneLake"}},
                {"path": "Files/other", "name": "outside", "target": {"type": "OneLake"}},
            ],
            "continuationToken": "next-page",
        },
    )
    page2 = _response(
        200,
        {"value": [{"path": "Files/managed", "name": "remove-two"}]},
    )
    with (
        patch.object(client, "create_shortcuts") as create,
        patch("requests.get", side_effect=[page1, page2]) as get,
        patch("requests.delete", return_value=_response(204)) as delete,
        patch("dbt.adapters.fabricspark.shortcuts.time.sleep") as sleep,
    ):
        shortcuts.create_and_prune_shortcuts(client, _config(configured), prune=True)

    create.assert_called_once()
    assert get.call_count == 2
    assert get.call_args_list[0].kwargs["params"] == {"parentPath": "Files/managed"}
    assert get.call_args_list[1].kwargs["params"] == {
        "parentPath": "Files/managed",
        "continuationToken": "next-page",
    }
    deleted_urls = [call.args[0] for call in delete.call_args_list]
    assert [url.rsplit("/", 1)[-1] for url in deleted_urls] == ["remove-one", "remove-two"]
    sleep.assert_not_called()


def test_empty_config_prunes_nothing_and_creation_failure_skips_pruning():
    client = _client()
    with (
        patch.object(client, "create_shortcuts"),
        patch("requests.get") as get,
        patch("requests.delete") as delete,
    ):
        shortcuts.create_and_prune_shortcuts(client, '{"shortcuts":[]}', prune=True)
    get.assert_not_called()
    delete.assert_not_called()

    with (
        patch.object(client, "create_shortcuts", side_effect=RuntimeError("creation failed")),
        patch("requests.get") as get,
        pytest.raises(RuntimeError, match="creation failed"),
    ):
        shortcuts.create_and_prune_shortcuts(client, _config(_shortcut()), prune=True)
    get.assert_not_called()


def test_create_and_prune_uses_file_config_for_both_operations():
    client = _client()
    config = json.dumps({"shortcuts": []})
    with (
        patch("builtins.open", mock_open(read_data=config)),
        patch.object(client, "create_shortcuts") as create,
        patch.object(client, "prune_shortcuts") as prune,
    ):
        shortcuts.create_and_prune_shortcuts(client, None, prune=True)

    create.assert_called_once_with(config)
    prune.assert_called_once_with(config)
