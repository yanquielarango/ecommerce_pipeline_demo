import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from transformations.silver.orders_functions import prepare_orders

ORDER_SCHEMA = StructType(
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

# A single valid order used as the baseline for every test below.
# Individual tests only override the field(s) they care about.
BASE_ORDER = {
    "order_id": "order-1001",
    "customer_id": "customer-10",
    "product_id": "product-200",
    "quantity": 2,
    "price": 100.0,
    "order_timestamp": "2026-08-25 10:00:00",
    "discount_code": None,
    "ingest_datetime": "2026-08-25 10:01:00",
}


def make_orders_df(spark: SparkSession, **overrides) -> DataFrame:
    """Build a single-row orders DataFrame from BASE_ORDER, with overrides."""
    order = {**BASE_ORDER, **overrides}
    return spark.createDataFrame([order], schema=ORDER_SCHEMA)


@pytest.mark.unit_test
def test_valid_order_is_not_quarantined(spark):
    result = prepare_orders(make_orders_df(spark)).first()

    assert result["is_quarantined"] is False
    assert result["quarantine_reason"] == ""


@pytest.mark.unit_test
def test_order_with_zero_price_is_not_quarantined(spark):
    """price == 0 is a valid boundary case (rule is price >= 0)."""
    result = prepare_orders(make_orders_df(spark, price=0.0)).first()

    assert result["is_quarantined"] is False
    assert result["quarantine_reason"] == ""


@pytest.mark.unit_test
@pytest.mark.parametrize(
    ("overrides", "expected_reason"),
    [
        ({"order_id": None}, "order_id is null"),
        ({"customer_id": None}, "customer_id is null"),
        ({"product_id": None}, "product_id is null"),
        ({"quantity": None}, "quantity is null"),
        ({"quantity": 0}, "quantity must be greater than 0"),
        ({"price": None}, "price is null"),
        ({"price": -10.0}, "price must be greater than or equal to 0"),
    ],
    ids=[
        "null-order-id",
        "null-customer-id",
        "null-product-id",
        "null-quantity",
        "zero-quantity",
        "null-price",
        "negative-price",
    ],
)
def test_invalid_order_is_quarantined(spark, overrides, expected_reason):
    result = prepare_orders(make_orders_df(spark, **overrides)).first()

    assert result["is_quarantined"] is True
    assert result["quarantine_reason"] == expected_reason