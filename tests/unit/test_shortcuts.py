import unittest
from unittest import mock

from dbt.adapters.fabricspark.shortcuts import Shortcut, ShortcutClient, TargetName


class TestShorcutClient(unittest.TestCase):
    def test_create_shortcut_does_not_exist_succeeds(self):
        # if check_if_exists_and_delete_shortcut returns false, create_shortcut posts
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch.object(client, "check_if_exists_and_delete_shortcut", return_value=False):
            with mock.patch("requests.post") as mock_post:
                mock_post.return_value.raise_for_status = mock.Mock()
                client.create_shortcut(shortcut)
                mock_post.assert_called_once()
                self.assertEqual(mock_post.call_args[0][0], "https://api.fabric.microsoft.com/v1/workspaces/workspace_id/items/item_id/shortcuts")

    def test_create_shortcut_exists_does_not_create(self):
        # if check_if_exists_and_delete_shortcut returns true, create_shortcut skips
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch.object(client, "check_if_exists_and_delete_shortcut", return_value=True):
            with mock.patch("requests.post") as mock_post:
                client.create_shortcut(shortcut)
                mock_post.assert_not_called()

    def test_check_if_exists_not_found_returns_false(self):
        # if response 404, check_if_exists_and_delete_shortcut returns False
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 404
            self.assertFalse(client.check_if_exists_and_delete_shortcut(shortcut))

    def test_check_if_exists_found_returns_true(self):
        # if response 200 and target matches, check_if_exists_and_delete_shortcut returns True
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.raise_for_status = mock.Mock()
            mock_get.return_value.json.return_value = {
                "path": "path",
                "name": "name",
                "target": {
                    "type": "OneLake",
                    "onelake": {
                        "workspaceId": "source_workspace_id",
                        "itemId": "source_item_id",
                        "path": "source_path"
                    }
                }
            }
            self.assertTrue(client.check_if_exists_and_delete_shortcut(shortcut))

    def test_check_if_exists_source_path_mismatch_returns_false_deletes_and_creates_new_shortcut(self):
        # if response 200 but target does not match, returns False and deletes old shortcut
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.raise_for_status = mock.Mock()
            mock_get.return_value.json.return_value = {
                "path": "path",
                "name": "name",
                "target": {
                    "type": "OneLake",
                    "onelake": {
                        "workspaceId": "source_workspace_id",
                        "itemId": "source_item_id",
                        "path": "wrong_source_path"
                    }
                }
            }
            with mock.patch.object(client, "delete_shortcut") as mock_delete:
                self.assertFalse(client.check_if_exists_and_delete_shortcut(shortcut))
                mock_delete.assert_called_once()
                self.assertEqual(mock_delete.call_args[0][0], "path")
                self.assertEqual(mock_delete.call_args[0][1], "name")
                # check that the client creates a new shortcut after deleting the old one
                with mock.patch.object(client, "check_if_exists_and_delete_shortcut", return_value=False):
                    with mock.patch("requests.post") as mock_post:
                        mock_post.return_value.raise_for_status = mock.Mock()
                        client.create_shortcut(shortcut)
                        mock_post.assert_called_once()
                        self.assertEqual(mock_post.call_args[0][0], "https://api.fabric.microsoft.com/v1/workspaces/workspace_id/items/item_id/shortcuts")

    def test_check_if_exists_source_workspace_id_mismatch_returns_false_deletes_and_creates_new_shortcut(self):
        # if response 200 but target does not match, returns False and deletes old shortcut
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.raise_for_status = mock.Mock()
            mock_get.return_value.json.return_value = {
                "path": "path",
                "name": "name",
                "target": {
                    "type": "OneLake",
                    "onelake": {
                        "workspaceId": "wrong_source_workspace_id",
                        "itemId": "source_item_id",
                        "path": "source_path"
                    }
                }
            }
            with mock.patch.object(client, "delete_shortcut") as mock_delete:
                self.assertFalse(client.check_if_exists_and_delete_shortcut(shortcut))
                mock_delete.assert_called_once()
                self.assertEqual(mock_delete.call_args[0][0], "path")
                self.assertEqual(mock_delete.call_args[0][1], "name")
                # check that the client creates a new shortcut after deleting the old one
                with mock.patch.object(client, "check_if_exists_and_delete_shortcut", return_value=False):
                    with mock.patch("requests.post") as mock_post:
                        mock_post.return_value.raise_for_status = mock.Mock()
                        client.create_shortcut(shortcut)
                        mock_post.assert_called_once()
                        self.assertEqual(mock_post.call_args[0][0], "https://api.fabric.microsoft.com/v1/workspaces/workspace_id/items/item_id/shortcuts")

    def test_check_if_exists_source_item_id_mismatch_returns_false_deletes_and_creates_new_shortcut(self):
        # if response 200 but target does not match, returns False and deletes old shortcut
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.raise_for_status = mock.Mock()
            mock_get.return_value.json.return_value = {
                "path": "path",
                "name": "name",
                "target": {
                    "type": "OneLake",
                    "onelake": {
                        "workspaceId": "source_workspace_id",
                        "itemId": "wrong_source_item_id",
                        "path": "source_path"
                    }
                }
            }
            with mock.patch.object(client, "delete_shortcut") as mock_delete:
                self.assertFalse(client.check_if_exists_and_delete_shortcut(shortcut))
                mock_delete.assert_called_once()
                self.assertEqual(mock_delete.call_args[0][0], "path")
                self.assertEqual(mock_delete.call_args[0][1], "name")
                # check that the client creates a new shortcut after deleting the old one
                with mock.patch.object(client, "check_if_exists_and_delete_shortcut", return_value=False):
                    with mock.patch("requests.post") as mock_post:
                        mock_post.return_value.raise_for_status = mock.Mock()
                        client.create_shortcut(shortcut)
                        mock_post.assert_called_once()
                        self.assertEqual(mock_post.call_args[0][0], "https://api.fabric.microsoft.com/v1/workspaces/workspace_id/items/item_id/shortcuts")

    def test_check_if_exists_error_raises_exception(self):
        # if response error, check_if_exists_and_delete_shortcut raises exception
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch("requests.get") as mock_get:
            mock_get.return_value.status_code = 500
            mock_get.return_value.raise_for_status.side_effect = Exception("Server Error")
            with self.assertRaises(Exception):
                client.check_if_exists_and_delete_shortcut(shortcut)

    def test_delete_shortcut_succeeds(self):
        # delete_shortcut calls requests.delete
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch("requests.delete") as mock_delete:
            mock_delete.return_value.raise_for_status = mock.Mock()
            with mock.patch("time.sleep"):  # skip the 30s poll wait
                client.delete_shortcut("path", "name")
            mock_delete.assert_called_once()
            self.assertEqual(mock_delete.call_args[0][0], "https://api.fabric.microsoft.com/v1/workspaces/workspace_id/items/item_id/shortcuts/path/name")
