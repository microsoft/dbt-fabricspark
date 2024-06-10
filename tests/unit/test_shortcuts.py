import unittest
from unittest import mock

from dbt.adapters.fabricspark.shortcuts import Shortcut, TargetName, ShortcutClient

class TestShorcutClient(unittest.TestCase):
    def test_create_shortcut_does_not_exist(self):
        # if check_exists false, create_shortcut succeeds
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch.object(client, "check_exists", return_value=False):
            with mock.patch("requests.post") as mock_post:
                client.create_shortcut(shortcut)
                mock_post.assert_called_once()
                self.assertEqual(mock_post.call_args[0][0], "https://api.fabric.microsoft.com/v1/workspaces/workspace_id/items/item_id/shortcuts")
    
    def test_create_shortcut_exists(self):
        # if check_exists true, create_shortcut does not get called
        shortcut = Shortcut(
            path="path",
            shortcut_name="name",
            target=TargetName.onelake,
            source_path="source_path",
            source_workspace_id="source_workspace_id",
            source_item_id="source_item_id"
        )
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch.object(client, "check_exists", return_value=True):
            with mock.patch("requests.post") as mock_post:
                client.create_shortcut(shortcut)
                mock_post.assert_not_called()

    def test_check_exists_not_found(self):
        # if response 404, check_exists returns False
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
            self.assertFalse(client.check_exists(shortcut))

    def test_check_exists_found(self):
        # if response 200, check_exists returns True
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
            mock_get.return_value.json.return_value = {
                "path": "path",
                "name": "name",
                "target": {
                    "onelake": {
                        "workspaceId": "source_workspace_id",
                        "itemId": "source_item_id",
                        "path": "source_path"
                    }
                }
            }
            self.assertTrue(client.check_exists(shortcut))
        
    def test_check_exists_source_path_mismatch(self):
        # if response 200 but target does not match, check_exists returns False
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
            mock_get.return_value.json.return_value = {
                "path": "path",
                "name": "name",
                "target": {
                    "onelake": {
                        "workspaceId": "source_workspace_id",
                        "itemId": "source_item_id",
                        "path": "wrong_source_path"
                    }
                }
            }
            with mock.patch.object(client, "delete_shortcut") as mock_delete:
                self.assertFalse(client.check_exists(shortcut))
                mock_delete.assert_called_once()
                self.assertEqual(mock_delete.call_args[0][0], "path")
                self.assertEqual(mock_delete.call_args[0][1], "name")
    
    def test_check_exists_source_workspace_id_mismatch(self):
        # if response 200 but target does not match, check_exists returns False
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
            mock_get.return_value.json.return_value = {
                "path": "path",
                "name": "name",
                "target": {
                    "onelake": {
                        "workspaceId": "wrong_source_workspace_id",
                        "itemId": "source_item_id",
                        "path": "source_path"
                    }
                }
            }
            with mock.patch.object(client, "delete_shortcut") as mock_delete:
                self.assertFalse(client.check_exists(shortcut))
                mock_delete.assert_called_once()
                self.assertEqual(mock_delete.call_args[0][0], "path")
                self.assertEqual(mock_delete.call_args[0][1], "name")
    
    def test_check_exists_source_item_id_mismatch(self):
        # if response 200 but target does not match, check_exists returns False
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
            mock_get.return_value.json.return_value = {
                "path": "path",
                "name": "name",
                "target": {
                    "onelake": {
                        "workspaceId": "source_workspace_id",
                        "itemId": "wrong_source_item_id",
                        "path": "source_path"
                    }
                }
            }
            with mock.patch.object(client, "delete_shortcut") as mock_delete:
                self.assertFalse(client.check_exists(shortcut))
                mock_delete.assert_called_once()
                self.assertEqual(mock_delete.call_args[0][0], "path")
                self.assertEqual(mock_delete.call_args[0][1], "name")
    
    def test_check_exists_error(self):
        # if response error, check_exists raises exception
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
            with self.assertRaises(Exception):
                client.check_exists(shortcut)
    
    def test_delete_shortcut(self):
        # delete_shortcut calls requests.delete
        client = ShortcutClient(token="token", workspace_id="workspace_id", item_id="item_id")
        with mock.patch("requests.delete") as mock_delete:
            client.delete_shortcut("path", "name")
            mock_delete.assert_called_once()
            self.assertEqual(mock_delete.call_args[0][0], "https://api.fabric.microsoft.com/v1/workspaces/workspace_id/items/item_id/shortcuts/path/name")
            