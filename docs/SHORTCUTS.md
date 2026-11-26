# Shortcuts

Create OneLake shortcuts in the profile's lakehouse when the adapter starts a Fabric session:

```yaml
create_shortcuts: true
shortcuts_json_file: config/shortcuts.json   # or shortcuts_json_str: '<json-string>'
prune_shortcuts: false
```

With `create_shortcuts: true`, supply either `shortcuts_json_str` or `shortcuts_json_file`, not both. `shortcuts_json_file` accepts an absolute path or a path relative to the directory where you run dbt. The file must contain the same JSON object as the inline setting, encoded as UTF-8. With neither setting, the adapter reads `./shortcuts.json`.

Shortcuts are applied when the adapter creates a new Fabric session; they are not reapplied when an existing session is reused. Shortcut creation failures fail the dbt run. Set `prune_shortcuts: true` to remove unlisted shortcuts only from parent folders named in the configuration. An empty shortcut list names no parent folders, so it prunes nothing.

---
[Home](../README.md) > [Documentation](./README.md)
