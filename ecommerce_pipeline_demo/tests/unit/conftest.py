import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("ecommerce-pipeline-unit-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()