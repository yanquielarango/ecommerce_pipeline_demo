import os

import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        DatabricksSession.builder
        .serverless(True)
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def catalog() -> str:
    return os.environ["DATABRICKS_CATALOG"]


@pytest.fixture(scope="session")
def silver_schema() -> str:
    return os.environ["DATABRICKS_SILVER_SCHEMA"]


@pytest.fixture(scope="session")
def gold_schema() -> str:
    return os.environ["DATABRICKS_GOLD_SCHEMA"]


@pytest.fixture(scope="session")
def silver_orders_table(catalog: str, silver_schema: str) -> str:
    return f"{catalog}.{silver_schema}.slv_orders"


@pytest.fixture(scope="session")
def gold_fact_orders_table(catalog: str, gold_schema: str) -> str:
    return f"{catalog}.{gold_schema}.fact_orders"