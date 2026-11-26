<!-- PROJECT LOGO -->
<p align="center">
  <img src="https://rakirahman.blob.core.windows.net/public/images/Misc/dbt-fabricspark.png" alt="Logo" width="30%">
  <h3 align="center">Fabric Spark - dbt</h3>
  <p align="center">
    dbt adapter for Fabric Spark supporting SQL models.
    <br />
    <br />
    <a href="https://docs.getdbt.com/">dbt Docs</a>
    ·
    <a href="https://learn.microsoft.com/en-us/fabric/data-engineering/tutorial-lakehouse-data-preparation?tabs=pyspark">Fabric Lakehouse with Spark</a>
    ·
    <a href="https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview">Fabric Lakehouse Livy API</a>
    <br />
    <br />
    <a href="https://github.com/microsoft/dbt-fabricspark/actions/workflows/ci.yml">
        <img src="https://github.com/microsoft/dbt-fabricspark/actions/workflows/ci.yml/badge.svg?branch=main" alt="Tests and Code Checks"/>
    </a>
    <a href="https://github.com/microsoft/dbt-fabricspark/actions/workflows/release.yml">
        <img src="https://github.com/microsoft/dbt-fabricspark/actions/workflows/release.yml/badge.svg" alt="Release to PyPI"/>
    </a>
    <br />
    <img src="https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue" alt="Python"/>
    <img src="https://img.shields.io/badge/dbt--core-%3E%3D1.8.0-orange" alt="dbt-core"/>
    <img src="https://img.shields.io/badge/license-MIT-green" alt="License"/>
  </p>
</p>

<br>

[dbt](https://www.getdbt.com/) enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-fabricspark

The `dbt-fabricspark` package contains all of the code enabling dbt to work with Apache Spark in Microsoft Fabric. This adapter connects to Fabric Lakehouses via Livy endpoints and supports both **schema-enabled** and **non-schema** Lakehouse configurations.

### Key Features

- **Livy session management** with session reuse and robust connectivity across dbt runs
- **Lakehouse with schema support** — auto-detects schema-enabled lakehouses and uses three-part naming (`lakehouse.schema.table`)
- **Lakehouse without schema** — standard two-part naming (`lakehouse.table`)
- **Materializations**: table, view, incremental (append, merge, insert_overwrite, microbatch, delete+insert), seed, snapshot
- **Fabric Environment support** via `environmentId` configuration
- **Security**: credential masking, UUID validation, HTTPS + domain validation, thread-safe token refresh
- **Resilience**: HTTP 5xx retry with exponential backoff, bounded polling with configurable timeouts

## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

> To contribute to this adapter codebase, see [CONTRIBUTING.md](CONTRIBUTING.md).

### Installation

```bash
pip install dbt-fabricspark
```

For local development using **Azure CLI** authentication (`authentication: CLI`), install with the `cli` extra:

```bash
pip install dbt-fabricspark[cli]
```

For an in-process Spark Session, install PySpark with the `spark` extra:

```bash
pip install "dbt-fabricspark[spark]"
```

PySpark remains optional: omit this extra when using Livy, or when the runtime
already provides PySpark, such as a Fabric Spark notebook. The session method
supports PySpark 3.5 and 4.x.

> **Note:** The `azure-cli` optional dependency is only required for the `CLI` authentication mode. Service Principal (`SPN`) and Fabric Notebook (`fabric_notebook`) authentication modes do not need it.

### Configuration

Add a target to `profiles.yml`. This example connects to a schema-enabled Lakehouse through Fabric Livy with Azure CLI authentication:

```yaml
fabric-spark-test:
  target: fabricspark-dev
  outputs:
    fabricspark-dev:
        type: fabricspark
        method: livy
        endpoint: https://api.fabric.microsoft.com/v1
        workspaceid: <your-workspace-id>
        lakehouseid: <your-lakehouse-id>
        lakehouse: my_lakehouse
        schema: dbo
        authentication: CLI
        threads: 4
        spark_config:
          name: my-dbt-session
```

See [Lakehouses](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/LAKEHOUSES.md) for how `lakehouse` and `schema` select two- or three-part naming, and the [Configuration Reference](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/CONFIGURATION.md) for every option.

## Documentation

| Guide                                                                                                                                       | Covers                                                                                                       |
| ------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| [Connections](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/CONNECTIONS.md)                                                   | Livy and in-process Spark Session connections, authentication modes and `spark_config`                       |
| [Lakehouses](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/LAKEHOUSES.md)                                                     | Lakehouses with and without schemas, schema detection, cross-lakehouse writes and case-sensitive identifiers |
| [Cross-Workspace 4-Part Naming](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/CROSS_WORKSPACE.md)                             | Reading from and writing to lakehouses in other workspaces                                                   |
| [Configuration Reference](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/CONFIGURATION.md)                                     | Every `profiles.yml` option                                                                                  |
| [Incremental strategies](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/INCREMENTAL.md)                                        | Incremental strategies, advanced `merge` options and `on_schema_change`                                      |
| [Automatic OPTIMIZE](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/AUTO_OPTIMIZE.md)                                          | Compacting Delta tables after each build, and turning it off                                                 |
| [Transient operation retry](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/RETRIES.md)                                         | Retrying Spark SQL statements and MLV refreshes that fail with a matching error                              |
| [High-concurrency Livy](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/HIGH_CONCURRENCY.md)                                    | Per-thread REPLs, session reuse and the `spark.highConcurrency.max` cap                                      |
| [Shortcuts](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/SHORTCUTS.md)                                                       | Creating and pruning OneLake shortcuts from a JSON definition                                                |
| [Publish descriptions to the Lakehouse Table overview](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/PERSIST_DOCS_ONELAKE.md) | Writing dbt descriptions to OneLake table metadata with `persist_docs_onelake`                               |
| [Materialized Lake Views](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/MATERIALIZED_LAKE_VIEWS.md)                           | The `materialized_lake_view` materialization, constraints and refresh schedules                              |
| [FAQs](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/docs/FAQ.md)                                                                  | Frequently asked questions                                                                                   |

## Issues, bug-bashing, help us help you

> ⚠️ Here's how you can get your issue triaged and fixed ASAP

In the age of AI, we should be innovating and shipping **high-quality** software daily.

So in this adapter, we try to fix bugs and ship features fast - and keep an extremely high bar for test coverage in CI before PRs merge to `main`.

Once you open an issue, please - try to be **as descriptive as possible** to give our human/AI maintainers the necessary details to reproduce the issue rapidly.

For example - if you can - create a dummy repro dbt project in your GitHub account - so the maintainers can reproduce your problem ASAP - see an example a well-written GitHub issue [here](https://github.com/microsoft/dbt-fabricspark/issues/172#issuecomment-4426907640).

If the issue is complex - once we have a fix identified, to gain more confidence, we might ask you install the adapter right from a PR branch to ensure the repro is gone in your setup as well, like so:

```bash
pip install git+https://github.com/microsoft/dbt-fabricspark.git@dev/somebranch/123
```

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know on [Slack](http://community.getdbt.com/), or open [an issue](https://github.com/microsoft/dbt-fabricspark/issues/new)
- Want to help us build dbt? Check out the [Contributing Guide](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/CONTRIBUTING.md)

## Join the dbt Community

- Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
- Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
