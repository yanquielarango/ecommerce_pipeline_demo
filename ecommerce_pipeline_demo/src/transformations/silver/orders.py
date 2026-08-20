import pyspark.sql.functions as F

from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.table(
    name=f"{SILVER_SCHEMA}.slv_orders",
    comment="Clean and validated order events from Zerobus Bronze ingestion",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop(
    "valid_order_id",
    "order_id IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_product_id",
    "product_id IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_quantity",
    "quantity > 0",
)
@dp.expect_or_drop(
    "valid_price",
    "price >= 0",
)
def orders_silver():
    return (
        spark.readStream.table(  # noqa: F821
            f"{CATALOG}.{BRONZE_SCHEMA}.brz_orders"
        )
        .withColumn(
            "order_timestamp",
            F.to_timestamp("order_timestamp"),
        )
        .withColumn(
            "ingest_datetime",
            F.to_timestamp("ingest_datetime"),
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