from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from transformations.silver.slv_orders_functions import prepare_orders

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("discount_code", StringType(), True),
        StructField("ingest_datetime", StringType(), True),
    ]
)


def test_prepare_orders_converts_timestamps_and_marks_invalid_record(spark):
    input_df = spark.createDataFrame(
        [
            (
                "O001",
                "C001",
                "P001",
                0,
                25.0,
                "2026-08-27T10:00:00+00:00",
                None,
                "2026-08-27T10:05:00+00:00",
            )
        ],
        schema=ORDERS_SCHEMA,
    )

    result = prepare_orders(input_df).collect()[0]

    assert result.order_timestamp is not None
    assert result.ingest_datetime is not None
    assert result.is_quarantined is True
    assert result.quarantine_reason == "quantity must be greater than 0"