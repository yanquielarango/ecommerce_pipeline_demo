import pyspark.sql.functions as F
from pyspark.sql import DataFrame

ORDER_DQ_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_quantity": "quantity IS NOT NULL AND quantity > 0",
    "valid_price": "price IS NOT NULL AND price >= 0",
}

VALID_ORDER_CONDITION = " AND ".join(f"({rule})" for rule in ORDER_DQ_RULES.values())


def prepare_orders(orders: DataFrame) -> DataFrame:
    return (
        orders.withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("ingest_datetime", F.to_timestamp("ingest_datetime"))
        .withColumn("is_quarantined", F.expr(f"NOT ({VALID_ORDER_CONDITION})"))
        .withColumn(
            "quarantine_reason",
            F.concat_ws(
                ", ",
                F.when(F.col("order_id").isNull(), F.lit("order_id is null")),
                F.when(F.col("customer_id").isNull(), F.lit("customer_id is null")),
                F.when(F.col("product_id").isNull(), F.lit("product_id is null")),
                F.when(F.col("quantity").isNull(), F.lit("quantity is null")),
                F.when(F.col("quantity") <= 0, F.lit("quantity must be greater than 0")),
                F.when(F.col("price").isNull(), F.lit("price is null")),
                F.when(F.col("price") < 0, F.lit("price must be greater than or equal to 0")),
            ),
        )
    )