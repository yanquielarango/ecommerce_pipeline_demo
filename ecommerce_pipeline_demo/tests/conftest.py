import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("ecommerce-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()