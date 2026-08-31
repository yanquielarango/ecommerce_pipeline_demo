from datetime import datetime

import pytest

from transformations.gold.fact_orders_functions import build_fact_orders


@pytest.mark.unit_test
def test_build_fact_orders_creates_keys_and_amounts(spark):
    orders = spark.createDataFrame(
        [
            (
                "O001",
                "C001",
                "P001",
                2,
                50.0,
                datetime(2026, 8, 27, 10, 0, 0),
                "WELCOME10",
            )
        ],
        [
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "price",
            "order_timestamp",
            "discount_code",
        ],
    )

    customers = spark.createDataFrame(
        [
            ("C001", 1001),
        ],
        [
            "customer_id",
            "customer_key",
        ],
    )

    products = spark.createDataFrame(
        [
            ("P001", 2001),
        ],
        [
            "product_id",
            "product_key",
        ],
    )

    result = build_fact_orders(
        orders,
        customers,
        products,
    ).collect()[0]

    assert result.date_key == 20260827
    assert result.customer_key == 1001
    assert result.product_key == 2001
    assert result.line_amount == 50.0
    assert result.unit_price == 25.0