# Contribution Guide for dbt-ClickHouse Adapter

## Introduction

Thank you for considering contributing to the dbt-ClickHouse adapter! We value your contributions and appreciate your
efforts to improve the adapter. This guide will help you get started with the contribution process.

## Getting Started

### 1. Fork the Repository

Start by forking the repository on GitHub. This will create a copy of the repository under your own GitHub account.

### 2. Set Up Environment

* Make sure Python is installed locally, please refer to [dbt's python compatibility](https://docs.getdbt.com/faqs/Core/install-python-compatibility) (We recommend using version 3.12+).
* Create a dedicated virtual environment (optional but recommended)
* Install all the development requirements and the local project as an editable package:
    ```bash
    pip install -e . -r dev_requirements.txt
    ```
* Verify the package was installed successfully:
  ```bash
    pip list | grep dbt-clickhouse
    ```
  the package will be directed to your local project.

### 3. Create a Branch

Create a new branch for your feature or bug fix, please make sure to follow 

```bash
git checkout -b my-new-feature
```

### 4. Make Your Changes

Make the necessary changes in your branch. Ensure that your code follows the existing style and conventions used in the
project.
We strongly recommend to stick to [this](https://www.conventionalcommits.org/en/v1.0.0/) official commit message conventions.

### 5. Add or Adjust Tests

The project tests are located under the `test` folder. Please look for the relevant test associated with
your changes, and adjust it. If not such test, please create one.

### 6. Run The Tests

See [Running Tests](#running-tests) for more information.

> **Important:** Please make sure the tests are running successfully before pushing your code.

### 7. Create a PR
Create a pull request from your forked repository to the main one, include the following:
* In case this is your first contribution, make sure to sign ClickHouse's CLA.
* Link the related issue to your PR.
* Add a sensible description of the feature/issue and detail the use case.
* Make sure to update [CHANGELOG.md](CHANGELOG.md).


# Running Tests

This adapter passes all of dbt basic tests as presented in dbt's [official docs](https://docs.getdbt.com/docs/contributing/testing-a-new-adapter#testing-your-adapter).
Use `pytest tests` to run tests.

You can customize the test environment via environment variables. We recommend doing so with the pytest `pytest-dotenv` plugin combined with root level `test.env`
configuration file (this file should not be checked into git).  The following environment variables are recognized:

1. DBT_CH_TEST_HOST - Default=`localhost`
2. DBT_CH_TEST_USER - your ClickHouse username. Default=`default`
3. DBT_CH_TEST_PASSWORD - your ClickHouse password. Default=''
4. DBT_CH_TEST_PORT - ClickHouse client port. Default=8123 (The default is automatically changed to the correct port if DBT_CH_TEST_USE_DOCKER is enabled)
5. DBT_CH_TEST_DB_ENGINE - Database engine used to create schemas.  Defaults to '' (server default)
6. DBT_CH_TEST_USE_DOCKER - Set to True to run clickhouse-server docker image (see [tests/integration/docker-compose.yml](./tests/integration/docker-compose.yml)).  Requires docker-compose. Default=False
7. DBT_CH_TEST_CH_VERSION - ClickHouse docker image to use.  Defaults to `latest`
8. DBT_CH_TEST_INCLUDE_S3 - Include S3 tests.  Default=False since these are currently dependent on a specific ClickHouse S3 bucket/test dataset
9. DBT_CH_TEST_CLUSTER_MODE - Use the profile value
10. DBT_CH_TEST_CLUSTER - ClickHouse cluster name, if DBT_CH_TEST_USE_DOCKER set to true, only `test_replica` and `test_shard` is valid (see tests/test_config.xml for cluster settings)
11. DBT_CH_TEST_DRIVER - Specifies the protocol for making requests to ClickHouse. Defaults to `http` but automatically switches to `native` when `DBT_CH_TEST_PORT` is set to `10900`, `9000` or `9440`.
