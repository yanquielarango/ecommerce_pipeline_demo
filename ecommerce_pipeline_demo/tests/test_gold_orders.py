from datetime import datetime

from transformations.gold.fact_orders_transform import (
    build_fact_orders,
)


def test_fact_orders_calculations(spark):
    orders = spark.createDataFrame(
        [
            (
                "order-1",
                "customer-1",
                "product-1",
                2,
                100.0,
                None,
                datetime(2026, 8, 24, 10, 0),
            )
        ],
        schema="""
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            price DOUBLE,
            discount_code STRING,
            order_timestamp TIMESTAMP
        """,
    )

    customers = spark.createDataFrame(
        [
            ("customer-1", 101),
        ],
        schema="""
            customer_id STRING,
            customer_key INT
        """,
    )

    products = spark.createDataFrame(
        [
            ("product-1", 201),
        ],
        schema="""
            product_id STRING,
            product_key INT
        """,
    )

    dates = spark.createDataFrame(
        [
            (20260824,),
        ],
        schema="""
            date_key INT
        """,
    )

    result = build_fact_orders(
        orders=orders,
        customers=customers,
        products=products,
        dates=dates,
    ).first()

    assert result["order_id"] == "order-1"
    assert result["date_key"] == 20260824
    assert result["customer_key"] == 101
    assert result["product_key"] == 201
    assert result["quantity"] == 2
    assert result["unit_price"] == 50.0
    assert result["line_amount"] == 100.0