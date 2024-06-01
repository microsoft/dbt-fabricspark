<a href="https://github.com/microsoft/dbt-fabricspark/actions/workflows/integration.yml">
  <img src="https://github.com/microsoft/dbt-fabricspark/actions/workflows/integration.yml/badge.svg?branch=main&event=pull_request" alt="Adapter Integration Tests"/>
</a>

<br>
[dbt](https://www.getdbt.com/) enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-fabricspark

The `dbt-fabricspark` package contains all of the code enabling dbt to work with Synapse Spark in Microsoft Fabric. For more information, consult [the docs](https://docs.getdbt.com/docs/profile-fabricspark).

## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

## Running locally
Use livy endpoint to connect to Synapse Spark in Microsoft Fabric. The binaries required to setup local environment is not possiblw with Synapse Spark in Microsoft Fabric. However, you can configure profile to connect via livy endpoints.

Create a profile like this one:

```yaml
fabric-spark-test:
  target: fabricspark-dev
  outputs:
    fabricspark-dev:
        authentication: CLI
        method: livy
        connect_retries: 0
        connect_timeout: 10
        endpoint: https://api.fabric.microsoft.com/v1
        workspaceid: bab084ca-748d-438e-94ad-405428bd5694
        lakehouseid: ccb45a7d-60fc-447b-b1d3-713e05f55e9a
        lakehouse: test
        schema: test
        threads: 1
        type: fabricspark
        retry_all: true
```

### Reporting bugs and contributing code

-   Want to report a bug or request a feature? Let us know on [Slack](http://slack.getdbt.com/), or open [an issue](https://github.com/microsoft/dbt-fabricspark/issues/new).

## Code of Conduct

Everyone interacting in the Microsoft project's codebases, issue trackers, and mailing lists is expected to follow the [PyPA Code of Conduct](https://www.pypa.io/en/latest/code-of-conduct/).

## Join the dbt Community

- Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
- Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know on [Slack](http://community.getdbt.com/), or open [an issue](https://github.com/microsoft/dbt-fabricspark/issues/new)
- Want to help us build dbt? Check out the [Contributing Guide](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/CONTRIBUTING.md)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
