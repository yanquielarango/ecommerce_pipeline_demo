import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def build_fact_orders(
    orders: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    dates: DataFrame,
) -> DataFrame:
    return (
        orders
        .withColumn(
            "date_key",
            F.date_format(
                "order_timestamp",
                "yyyyMMdd",
            ).cast("int"),
        )
        .alias("o")
        .join(
            customers.alias("c"),
            F.col("o.customer_id")
            == F.col("c.customer_id"),
            "left",
        )
        .join(
            products.alias("p"),
            F.col("o.product_id")
            == F.col("p.product_id"),
            "left",
        )
        .join(
            dates,
            "date_key",
            "left",
        )
        .withColumn(
            "line_amount",
            F.col("o.price"),
        )
        .withColumn(
            "unit_price",
            F.when(
                F.col("o.quantity") > 0,
                F.col("o.price") / F.col("o.quantity"),
            ),
        )
        .select(
            F.col("o.order_id").alias("order_id"),
            "date_key",
            F.col("c.customer_key").alias(
                "customer_key"
            ),
            F.col("p.product_key").alias(
                "product_key"
            ),
            F.col("o.quantity").alias("quantity"),
            "unit_price",
            "line_amount",
            F.col("o.discount_code").alias(
                "discount_code"
            ),
            F.col("o.order_timestamp").alias(
                "order_timestamp"
            ),
        )
    )