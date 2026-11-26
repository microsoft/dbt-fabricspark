# Publish descriptions to the Lakehouse Table overview

Spark comments from `persist_docs` are unchanged by default. To also update the [OneLake additional table metadata API (preview)](https://learn.microsoft.com/fabric/onelake/table-apis/additional-table-metadata), enable the resource config in `dbt_project.yml`:

```yaml
models:
  +persist_docs_onelake: true
  +persist_docs: {relation: true, columns: true}
```

Set the same keys under `seeds` or `snapshots` to opt those resources in. Delta tables and incremental models, seeds and snapshots are supported; views, materialized lake views and clones are not. Only nonempty YAML descriptions selected by `relation` or `columns` are published. Each description may contain up to 1,024 characters. The profile identity must be able to read and write the target metadata and acquire a token for Azure Storage (CLI, SPN, `fabric_notebook` or `token_credential`).

The adapter reads existing metadata before replacing it, retaining tags and unselected descriptions. Coordinate other metadata writers: the API does not provide an atomic merge. An authentication, validation or sync failure fails the dbt operation; it does not roll back data already written.

Reads are retried on throttling and transient errors. Each write replaces all metadata and has no idempotency key, so it is sent once and never replayed. After a timeout, connection loss, HTTP 408 or 5xx, the write may still complete; dbt fails and reports the outcome as unknown. Rerunning dbt reads the current metadata and merges again.

Models written to another workspace or lakehouse are resolved by name with [List Workspaces](https://learn.microsoft.com/rest/api/fabric/core/workspaces/list-workspaces) and [List Lakehouses](https://learn.microsoft.com/rest/api/fabric/lakehouse/items/list-lakehouses). The identity must be able to list both (delegated scope `Workspace.Read.All` or `Workspace.ReadWrite.All`; List Lakehouses documents the Viewer workspace role), and the profile `endpoint` must be an HTTPS Fabric API host such as `https://api.fabric.microsoft.com/v1`. Tables in the profile's own lakehouse need no listing permissions.

---
[Home](../README.md) > [Documentation](./README.md)
