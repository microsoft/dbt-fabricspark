# Connections

Configure `profiles.yml` to connect through Livy or an in-process Spark Session.

## Connection Modes

The adapter supports two connection methods:

- **Livy** (`method: livy`) — Connects through the Livy API. `livy_mode: fabric`
  targets Microsoft Fabric, while `livy_mode: local` targets a self-hosted Livy
  server. Local Livy supports `reuse_session` and does not require Fabric compute.

- **Spark Session** (`method: session`) — Uses `SparkSession.builder` in the dbt
  process. This works with a customer-installed PySpark 3.5 or 4.x package and
  reuses an existing runtime-provided session when one is available.

For Fabric Livy development workflows, enable `reuse_session: true` to persist
the Livy session ID to a local file (`session_id_file`, default
`./livy-session-id.txt`). Later dbt runs reuse that session when it is still valid.

## In-process Spark Session

The session method needs no Fabric endpoint or credentials. `spark_config.name`
sets the Spark application name, and entries under `spark_config.conf` are
applied through `SparkSession.builder.config`. Hive support is enabled so the
session can use a configured persistent metastore.

```yaml
local-spark:
  target: dev
  outputs:
    dev:
      type: fabricspark
      method: session
      schema: dbt_local
      threads: 4
      spark_config:
        name: dbt-local-session
        conf:
          spark.master: local[4]
```

Spark 4 enables ANSI SQL mode by default while Spark 3.5 does not. To keep dbt
behavior consistent across both versions, session connections set
`spark.sql.ansi.enabled` to `false`.

## Authentication Modes

| Mode                  | Value             | Use Case                                                                | Required Fields                           |
| --------------------- | ----------------- | ----------------------------------------------------------------------- | ----------------------------------------- |
| **Azure CLI**         | `CLI`             | Local development. Uses `az login` credentials.                         | None (run `az login` first)               |
| **Service Principal** | `SPN`             | CI/CD and automation. Uses Azure AD app registration.                   | `client_id`, `tenant_id`, `client_secret` |
| **Fabric Notebook**   | `fabric_notebook` | Running dbt inside a Fabric notebook. Uses `notebookutils.credentials`. | None (runs in Fabric runtime)             |

## Inspecting `spark_config`

For `method: livy`, `spark_config` is forwarded to the session-create call in
high-concurrency, singleton Fabric, and local modes. In high-concurrency mode,
a non-empty `artifactName` causes the adapter to omit `name` from the acquire
request so Fabric can surface the job in Monitoring Hub as
`HC_<LakehouseName>_<LIVY_SESSION_ID>`. The profile still requires `name`;
singleton Fabric, local Livy, and HC requests without a non-empty
`artifactName` continue to receive it unchanged.

Only `sessionTag` is adapter-owned, and `spark.fabric.environment.id` /
`spark.livy.session.idle.timeout` are merged into `conf` when `environmentId` /
`session_idle_timeout` are set.

For `method: session`, `spark_config.name` becomes the application name and each
`spark_config.conf` entry is applied to the Spark builder. The adapter pins
`spark.sql.ansi.enabled` to `false` for consistent Spark 3.5/4.x behavior.

To see exactly what was sent:

```bash
dbt debug          # echoes spark_config and high_concurrency
dbt run --debug    # logs the full session-create payload
```

If a `conf` key does not appear to take effect, the payload log tells you whether the adapter
sent it. A Fabric [resource profile](https://learn.microsoft.com/en-us/fabric/data-engineering/configure-resource-profile-configurations)
is applied _after_ the session `conf`, so a key the profile defines (for example
`spark.sql.parquet.vorder.default`) overrides the value requested in `spark_config.conf`.
Keys the profile does not define are unaffected.

---
[Home](../README.md) > [Documentation](./README.md)
