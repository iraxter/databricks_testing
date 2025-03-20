
# Databricks Python SQL Connector

## Intro

This is a basic template to set up Python infrastructure to run SQL syntax in a Databricks environment. Needs a functioning Databricks instance and is currently set up for a Serverless SQL Warehouse but should work with a Databricks Cluster without much change.

### Setup

1. Clone Repo
2. Install pyenv and pipenv if not already installed
3. Create a file called creds.yaml in main directory using creds_template.yaml for sample formatting and populate with credentials for your specific Databricks instance.
    - *Note: creds.yaml is included in .gitignore for security reasons which is why a local version needs to be made. This is a temporary option and we'll likely want a better long-term option for credendial management*

### Contents

- test.py
    - scratch pad testing basic code and executing functions from utils.py
- utils.py
    - utility functions for executing sql strings via function call, creating and populating tables from list inputs, and more. will continue to add to this, may explore additional functions or expanding to a class in the future.
- test_csv.csv
    - test file to read into csv_to_sql function

### Additional Documentation

- [Databricks docs](https://docs.databricks.com/aws/en/dev-tools/python-sql-connector)
- [Databricks SQL Connector for Python repo](https://github.com/databricks/databricks-sql-python)

#### Future Ideas

- [PyArrow](https://arrow.apache.org/docs/python/index.html) - Expanded control specific to Spark and Databricks. The default SQL connector is built on Arrow and PyArrow allows more granular control of the API.
- [SQLAlchemy connector](https://docs.databricks.com/aws/en/dev-tools/sqlalchemy) - Much more flexibility for stuff originally designed in Python but slightly less performant and unnecessarily complex for current use cases (running already written SQL strings)