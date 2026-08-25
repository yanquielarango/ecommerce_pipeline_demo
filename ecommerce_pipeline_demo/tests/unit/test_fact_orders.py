from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformations.gold.fact_orders_functions import build_fact_orders

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount_code", StringType(), True),
        StructField("order_timestamp", TimestampType(), True),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_key", LongType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_key", LongType(), True),
    ]
)

DATES_SCHEMA = StructType(
    [
        StructField("date_key", IntegerType(), True),
    ]
)


BASE_ORDER = {
    "order_id": "order-1001",
    "customer_id": "customer-10",
    "product_id": "product-200",
    "quantity": 2,
    "price": 100.0,
    "discount_code": None,
    "order_timestamp": datetime(2026, 8, 25, 10, 0, 0),
}

BASE_CUSTOMER = {
    "customer_id": "customer-10",
    "customer_key": 101,
}

BASE_PRODUCT = {
    "product_id": "product-200",
    "product_key": 201,
}

BASE_DATE = {
    "date_key": 20260825,
}


def make_orders_df(
    spark: SparkSession,
    **overrides,
) -> DataFrame:
    """Build a single-row Silver orders DataFrame."""
    order = {
        **BASE_ORDER,
        **overrides,
    }

    return spark.createDataFrame(
        [order],
        schema=ORDERS_SCHEMA,
    )


def make_customers_df(
    spark: SparkSession,
    rows: list[dict] | None = None,
) -> DataFrame:
    """Build a customer dimension DataFrame."""
    data = [BASE_CUSTOMER] if rows is None else rows

    return spark.createDataFrame(
        data,
        schema=CUSTOMERS_SCHEMA,
    )


def make_products_df(
    spark: SparkSession,
    rows: list[dict] | None = None,
) -> DataFrame:
    """Build a product dimension DataFrame."""
    data = [BASE_PRODUCT] if rows is None else rows

    return spark.createDataFrame(
        data,
        schema=PRODUCTS_SCHEMA,
    )


def make_dates_df(
    spark: SparkSession,
    rows: list[dict] | None = None,
) -> DataFrame:
    """Build a date dimension DataFrame."""
    data = [BASE_DATE] if rows is None else rows

    return spark.createDataFrame(
        data,
        schema=DATES_SCHEMA,
    )


@pytest.mark.unit_test
def test_build_fact_orders_creates_expected_fact_row(
    spark: SparkSession,
):
    result = build_fact_orders(
        orders=make_orders_df(spark),
        customers=make_customers_df(spark),
        products=make_products_df(spark),
        dates=make_dates_df(spark),
    )

    rows = result.take(2)

    assert len(rows) == 1

    row = rows[0]

    assert row["order_id"] == "order-1001"
    assert row["date_key"] == 20260825
    assert row["customer_key"] == 101
    assert row["product_key"] == 201
    assert row["quantity"] == 2
    assert row["unit_price"] == 50.0
    assert row["line_amount"] == 100.0


@pytest.mark.unit_test
def test_build_fact_orders_keeps_order_when_dimensions_are_missing(
    spark: SparkSession,
):
    result = build_fact_orders(
        orders=make_orders_df(
            spark,
            order_id="order-1002",
            customer_id="customer-missing",
            product_id="product-missing",
            quantity=1,
            price=75.0,
        ),
        customers=make_customers_df(
            spark,
            rows=[],
        ),
        products=make_products_df(
            spark,
            rows=[],
        ),
        dates=make_dates_df(spark),
    )

    rows = result.take(2)

    assert len(rows) == 1

    row = rows[0]

    assert row["order_id"] == "order-1002"
    assert row["customer_key"] is None
    assert row["product_key"] is None
    assert row["date_key"] == 20260825
    assert row["line_amount"] == 75.0
    assert row["unit_price"] == 75.0