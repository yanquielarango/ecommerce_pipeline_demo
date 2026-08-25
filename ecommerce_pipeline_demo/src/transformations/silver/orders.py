import pyspark.sql.functions as F

from pyspark import pipelines as dp

from transformations.silver.orders_functions import (
    ORDER_DQ_RULES,
    prepare_orders,
)


CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.temporary_view(
    name="orders_validated",
    comment=(
        "Order events prepared and evaluated "
        "against Silver data quality rules"
    ),
)
@dp.expect_all(ORDER_DQ_RULES)
def orders_validated():
    source_df = spark.readStream.table(  # noqa: F821
        f"{CATALOG}.{BRONZE_SCHEMA}.brz_orders"
    )

    return (
        prepare_orders(source_df)
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "price",
            "order_timestamp",
            "discount_code",
            "ingest_datetime",
            "is_quarantined",
            "quarantine_reason",
        )
    )


@dp.table(
    name=f"{SILVER_SCHEMA}.slv_orders",
    comment=(
        "Clean and validated order events "
        "from Zerobus Bronze ingestion"
    ),
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
def orders_silver():
    return (
        spark.readStream.table(  # noqa: F821
            "orders_validated"
        )
        .filter(
            F.col("is_quarantined") == F.lit(False)
        )
        .drop(
            "is_quarantined",
            "quarantine_reason",
        )
        .withWatermark(
            "ingest_datetime",
            "1 day",
        )
        .dropDuplicatesWithinWatermark(
            [
                "order_id",
                "product_id",
            ]
        )
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "price",
            "order_timestamp",
            "discount_code",
            "ingest_datetime",
        )
    )


@dp.table(
    name=f"{SILVER_SCHEMA}.quarantine_orders",
    comment=(
        "Order events rejected by "
        "Silver data quality rules"
    ),
    table_properties={
        "quality": "quarantine",
        "layer": "silver",
    },
)
def quarantine_orders():
    return (
        spark.readStream.table(  # noqa: F821
            "orders_validated"
        )
        .filter(
            F.col("is_quarantined") == F.lit(True)
        )
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "price",
            "order_timestamp",
            "discount_code",
            "ingest_datetime",
            "quarantine_reason",
        )
    )