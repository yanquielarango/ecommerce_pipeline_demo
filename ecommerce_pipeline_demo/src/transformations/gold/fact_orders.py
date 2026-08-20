from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema")  # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.fact_orders",
    comment="Fact table of orders one record per order product",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def fact_orders():
    orders = dp.read(f"{SILVER_SCHEMA}.slv_orders")
    customers = dp.read(f"{GOLD_SCHEMA}.dim_customer")
    products = dp.read(f"{GOLD_SCHEMA}.dim_product")
    dates = dp.read(f"{GOLD_SCHEMA}.dim_date")

    return (
        orders
        .withColumn("date_key", F.date_format("order_timestamp", "yyyyMMdd").cast("int"))
        .alias("o")
        .join(
            customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left",
        )
        .join(
            products.alias("p"),
            F.col("o.product_id") == F.col("p.product_id"),
            "left",
        )
        .join(dates.alias("d"), "date_key", "left")
        .withColumn("line_amount", F.col("o.price"))
        .withColumn(
            "unit_price",
            F.when(F.col("o.quantity") > 0, F.col("o.price") / F.col("o.quantity")),
        )
        .select(
            F.col("o.order_id").alias("order_id"),
            "date_key",
            F.col("c.customer_key").alias("customer_key"),
            F.col("p.product_key").alias("product_key"),
            F.col("o.quantity").alias("quantity"),
            "unit_price",
            "line_amount",
            F.col("o.discount_code").alias("discount_code"),
            F.col("o.order_timestamp").alias("order_timestamp"),
        )
    )