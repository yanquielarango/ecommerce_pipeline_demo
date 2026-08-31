import pytest
import pyspark.sql.functions as F


@pytest.mark.integration_test
def test_silver_orders_are_available_in_fact_orders(
    spark,
    silver_orders_table,
    gold_fact_orders_table,
):
    silver = (
        spark.table(silver_orders_table)
        .select("order_id")
        .distinct()
    )

    fact = (
        spark.table(gold_fact_orders_table)
        .select("order_id")
        .distinct()
    )

    assert silver.count() > 0
    assert fact.count() > 0

    missing_orders = (
        silver.alias("s")
        .join(
            fact.alias("f"),
            F.col("s.order_id") == F.col("f.order_id"),
            "left_anti",
        )
        .count()
    )

    assert missing_orders == 0