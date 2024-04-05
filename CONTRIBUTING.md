# Contributing to `dbt-spark`

1. [About this document](#about-this-document)
3. [Getting the code](#getting-the-code)
5. [Running `dbt-fabricspark` in development](#running-dbt-fabricspark-in-development)
6. [Testing](#testing)
7. [Updating Docs](#updating-docs)
7. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document
This document is a guide intended for folks interested in contributing to `dbt-fabricspark`. Below, we document the process by which members of the community should create issues and submit pull requests (PRs) in this repository. It is not intended as a guide for using `dbt-fabricspark`, and it assumes a certain level of familiarity with Python concepts such as virtualenvs, `pip`, Python modules, and so on. This guide assumes you are using macOS or Linux and are comfortable with the command line.

For those wishing to contribute we highly suggest reading the dbt-core's [contribution guide](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md) if you haven't already. Almost all of the information there is applicable to contributing here, too!

## Getting the code

You will need `git` in order to download and modify the `dbt-fabricspark` source code. You can find directions [here](https://github.com/git-guides/install-git) on how to install `git`.

### External contributors

If you are not a member of the `Microsoft` GitHub organization, you can contribute to `dbt-fabricspark` by forking the `dbt-fabricspark` repository. For a detailed overview on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. fork the `dbt-fabricspark` repository
2. clone your fork locally
3. check out a new branch for your proposed changes
4. push changes to your fork
5. open a pull request against `microsoft/dbt-fabricspark` from your forked repository

### Microsoft Org contributors

If you are a member of the `Microsoft` GitHub organization, you will have push access to the `dbt-fabricspark` repo. Rather than forking `dbt-fabricspark` to make your changes, just clone the repository, check out a new branch, and push directly to that branch.


## Running `dbt-fabricspark` in development

### Installation

First make sure that you set up your `virtualenv` as described in [Setting up an environment](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md#setting-up-an-environment).  Ensure you have the latest version of pip installed with `pip install --upgrade pip`. Next, install `dbt-fabricspark` latest dependencies:

```sh
pip install -e . -r dev-requirements.txt
```

When `dbt-fabricspark` is installed this way, any changes you make to the `dbt-fabricspark` source code will be reflected immediately in your next `dbt-fabricspark` run.

To confirm you have correct version of `dbt-core` installed please run `dbt --version` and `which dbt`.


## Testing

### Initial Setup

`dbt-fabricspark` uses test credentials specified in a `test.env` file in the root of the repository. This `test.env` file is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing. To create your `test.env` file, copy the provided example file, then supply your relevant credentials.

```
cp test.env.example test.env
$EDITOR test.env
```

### Test commands
There are a few methods for running tests locally.

#### `tox`
`tox` takes care of managing Python virtualenvs and installing dependencies in order to run tests. You can also run tests in parallel, for example you can run unit tests for Python 3.8, Python 3.9, and `flake8` checks in parallel with `tox -p`. Also, you can run unit tests for specific python versions with `tox -e <env>`. The configuration of these tests are located in `tox.ini`.

#### `pytest`
Finally, you can also run a specific test or group of tests using `pytest` directly. With a Python virtualenv active and dev dependencies installed you can do things like:

```sh
# run all functional tests
python -m pytest --profile az_cli tests/functional/
# run specific functional tests
python -m pytest --profile az_cli tests/functional/adapter/basic/*
# run all unit tests in a file
python -m pytest tests/unit/test_adapter.py
# run a specific unit test
python -m pytest test/unit/test_adapter.py::TestSparkAdapter::test_profile_with_database
```
## Updating Docs

Many changes will require and update to the `dbt-fabricspark` docs here are some useful resources.

- Docs are [here](https://docs.getdbt.com/).
- The docs repo for making changes is located [here]( https://github.com/dbt-labs/docs.getdbt.com).
- The changes made are likely to impact one or both of [Fabric Spark Profile](https://docs.getdbt.com/reference/warehouse-profiles/fabricspark-profile), or [Saprk Configs](https://docs.getdbt.com/reference/resource-configs/spark-configs).
- We ask every community member who makes a user-facing change to open an issue or PR regarding doc changes.

## Adding CHANGELOG Entry

Changelogs are managed manually for now. As you raise a PR, provide the changes made in your commits.

## Submitting a Pull Request

Microsoft provides a CI environment to test changes to the `dbt-fabricspark` adapter, and periodic checks against the development version of `dbt-core` through Github Actions.

A `dbt-fabricspark` maintainer will review your PR. They may suggest code revision for style or clarity, or request that you add unit or functional test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Once all requests and answers have been answered the `dbt-fabricspark` maintainer can trigger CI testing.

Once all tests are passing and your PR has been approved, a `dbt-fabricspark` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada:
