from datetime import datetime

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformations.silver.slv_customers_functions import prepare_customers

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", IntegerType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
        StructField("ingest_datetime", TimestampType(), True),
    ]
)


def test_prepare_customers_cleans_city_and_state(spark):
    input_df = spark.createDataFrame(
        [
            (
                "C001",
                "U001",
                12345,
                "  sao paulo  ",
                " sp ",
                None,
                datetime(2026, 8, 27, 10, 0, 0),
            )
        ],
        schema=CUSTOMERS_SCHEMA,
    )

    result = prepare_customers(input_df).collect()[0]

    assert result.customer_city == "sao paulo"
    assert result.customer_state == "SP"


def test_prepare_customers_removes_corrupt_records(spark):
    input_df = spark.createDataFrame(
        [
            (
                "C001",
                "U001",
                12345,
                "sao paulo",
                "SP",
                None,
                datetime(2026, 8, 27, 10, 0, 0),
            ),
            (
                "C002",
                "U002",
                54321,
                "rio de janeiro",
                "RJ",
                "corrupt row",
                datetime(2026, 8, 27, 10, 0, 0),
            ),
        ],
        schema=CUSTOMERS_SCHEMA,
    )

    result = prepare_customers(input_df).collect()

    assert len(result) == 1
    assert result[0].customer_id == "C001"