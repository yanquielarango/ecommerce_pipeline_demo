from transformations.silver.orders_transform import (
    prepare_orders,
)


ORDER_SCHEMA = """
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    price DOUBLE,
    order_timestamp STRING,
    discount_code STRING,
    ingest_datetime STRING
"""


def test_valid_order_passes_quality_rules(spark):
    source_df = spark.createDataFrame(
        [
            (
                "order-1",
                "customer-1",
                "product-1",
                2,
                100.0,
                "2026-08-24 10:00:00",
                None,
                "2026-08-24 10:01:00",
            )
        ],
        schema=ORDER_SCHEMA,
    )

    result = prepare_orders(source_df).first()

    assert result["is_quarantined"] is False
    assert result["quarantine_reason"] == ""


def test_invalid_quantity_is_quarantined(spark):
    source_df = spark.createDataFrame(
        [
            (
                "order-1",
                "customer-1",
                "product-1",
                0,
                100.0,
                "2026-08-24 10:00:00",
                None,
                "2026-08-24 10:01:00",
            )
        ],
        schema=ORDER_SCHEMA,
    )

    result = prepare_orders(source_df).first()

    assert result["is_quarantined"] is True
    assert (
        result["quarantine_reason"]
        == "quantity must be greater than 0"
    )