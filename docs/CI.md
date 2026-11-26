# Continuous integration

[`ci.yml`](../.github/workflows/ci.yml) runs in the pinned devcontainer image. It
runs `npx nx affected` for `build`, `lint` and `test` with `--configuration=ci`, then
`npm run fail-on-untracked-files`. Functional tests run against two MSIT workspaces
through `FABRIC_API`.

## Authentication

CI signs in through an IMDS relay instead of a GitHub OIDC service principal. A
relay host with a managed identity issues tokens over Azure Relay, and the job
reaches it through a loopback bridge in [`tools/scripts/ci/`](../tools/scripts/ci/):

1. `imds_relay.py start` starts `imds_relay_router.py` on `127.0.0.1`. The router
   exposes the App Service managed identity endpoint. Later steps receive
   `IDENTITY_ENDPOINT`, `IDENTITY_HEADER` and `IMDS_METADATA_ENDPOINT` through
   `$GITHUB_ENV`, and the header value is masked.
2. It runs `az login --identity --allow-no-subscriptions` with a run-owned
   `AZURE_CONFIG_DIR`, adding `--client-id` only when `UAMI_CLIENT_ID` is set. The
   adapter's `az_cli` profile and the functional tests then use Azure CLI tokens
   exactly as they do on a developer machine.
3. It obtains the Power BI token (Fabric REST and Livy) and the storage token
   (OneLake). A background process keeps both tokens fresh and logs every failed
   refresh. The router caches tokens per audience and client ID, refreshes them
   before they expire, and retries transient relay errors within a bounded time.
4. `imds_relay.py preflight` checks read access to both test workspaces before any
   build or test runs.
5. `imds_relay.py stop` runs even after a failure. It stops only the processes it
   started, removes its temporary state, and reports any failed refreshes.

Error output never includes relay URLs, SAS values or tokens.

## Setup

| Name                                 | Kind                | Scope                     | Purpose                                                                                                                |
| ------------------------------------ | ------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `IMDS_RELAY_TEMPORARY_SAS_URL`       | Secret              | `manual-run` environment  | HTTPS Azure Relay sender URL with a SAS token                                                                          |
| `UAMI_CLIENT_ID`                     | Variable (optional) | Repository or environment | Client ID of a user-assigned managed identity on the relay host. When unset, the relay host's default identity is used |
| `WORKSPACE_ID_*`, `WORKSPACE_NAME_*` | Secrets             | Repository                | The two test workspaces                                                                                                |

The relay identity needs a role that can create and delete lakehouses in both
workspaces. GitHub does not pass secrets to workflows from forks, so for
pull requests from forks the start step fails with a clear message.

The router cannot renew the SAS. Mint a sender URL with enough lifetime for the
runs it must cover, and renew the secret before it expires. Paste the URL at the
prompt, never on the command line:

```bash
gh secret set IMDS_RELAY_TEMPORARY_SAS_URL --env manual-run --repo microsoft/dbt-fabricspark
```

## Local runs

Locally, `az login` provides the same Azure CLI credentials, so the bridge is not
needed. Its unit tests use a fake relay:

```bash
uv run pytest tests/unit/test_imds_relay.py -vv
```

The router and bridge are adapted from
[openivm/ivm-bench](https://github.com/openivm/ivm-bench/tree/935e5f3d189c1a87fb409888cc357f676803581d)
under the MIT License.

---
[Home](../README.md) > [Documentation](./README.md)
