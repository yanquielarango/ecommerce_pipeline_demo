import os

import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark_session = (
        DatabricksSession.builder
        .serverless()
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


@pytest.fixture(scope="session")
def catalog() -> str:
    return os.environ["DATABRICKS_CATALOG"]


@pytest.fixture(scope="session")
def bronze_schema() -> str:
    return os.environ["DATABRICKS_BRONZE_SCHEMA"]


@pytest.fixture(scope="session")
def silver_schema() -> str:
    return os.environ["DATABRICKS_SILVER_SCHEMA"]


@pytest.fixture(scope="session")
def gold_schema() -> str:
    return os.environ["DATABRICKS_GOLD_SCHEMA"]


@pytest.fixture(scope="session")
def bronze_orders_table(catalog, bronze_schema) -> str:
    return f"{catalog}.{bronze_schema}.brz_orders"


@pytest.fixture(scope="session")
def silver_orders_table(catalog, silver_schema) -> str:
    return f"{catalog}.{silver_schema}.slv_orders"


@pytest.fixture(scope="session")
def quarantine_orders_table(catalog, silver_schema) -> str:
    return f"{catalog}.{silver_schema}.quarantine_orders"


@pytest.fixture(scope="session")
def gold_fact_orders_table(catalog, gold_schema) -> str:
    return f"{catalog}.{gold_schema}.fact_orders"