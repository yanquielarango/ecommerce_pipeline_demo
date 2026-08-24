import pyspark.sql.functions as F
from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


ORDER_DQ_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
    "valid_price": "price IS NOT NULL AND price >= 0",
}

VALID_ORDER_CONDITION = " AND ".join(
    f"({rule})"
    for rule in ORDER_DQ_RULES.values()
)


@dp.temporary_view(
    name="orders_validated",
    comment="Order events prepared and evaluated against Silver data quality rules",
)
@dp.expect_all(ORDER_DQ_RULES)
def orders_validated():
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
        .withColumn(
            "is_quarantined",
            F.expr(
                f"NOT ({VALID_ORDER_CONDITION})"
            ),
        )
        .withColumn(
            "quarantine_reason",
            F.concat_ws(
                ", ",
                F.when(
                    F.col("order_id").isNull(),
                    F.lit("order_id is null"),
                ),
                F.when(
                    F.col("customer_id").isNull(),
                    F.lit("customer_id is null"),
                ),
                F.when(
                    F.col("product_id").isNull(),
                    F.lit("product_id is null"),
                ),
                F.when(
                    F.col("quantity").isNull(),
                    F.lit("quantity is null"),
                ),
                F.when(
                    F.col("quantity") <= 0,
                    F.lit("quantity must be greater than 0"),
                ),
                F.when(
                    F.col("price").isNull(),
                    F.lit("price is null"),
                ),
                F.when(
                    F.col("price") < 0,
                    F.lit("price must be greater than or equal to 0"),
                ),
            ),
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
            "is_quarantined",
            "quarantine_reason",
        )
    )


@dp.table(
    name=f"{SILVER_SCHEMA}.slv_orders",
    comment="Clean and validated order events from Zerobus Bronze ingestion",
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
    comment="Order events rejected by Silver data quality rules",
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