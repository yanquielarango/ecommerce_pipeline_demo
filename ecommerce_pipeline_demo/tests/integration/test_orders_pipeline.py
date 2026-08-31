import pytest


@pytest.mark.integration_test
def test_orders_are_routed_to_silver_and_quarantine(
    spark,
    bronze_orders_table,
    silver_orders_table,
    quarantine_orders_table,
):
    bronze = spark.table(bronze_orders_table)
    silver = spark.table(silver_orders_table)
    quarantine = spark.table(quarantine_orders_table)

    valid_condition = """
        order_id IS NOT NULL
        AND customer_id IS NOT NULL
        AND product_id IS NOT NULL
        AND quantity IS NOT NULL
        AND quantity > 0
        AND price IS NOT NULL
        AND price >= 0
    """

    assert bronze.count() > 0
    assert silver.count() > 0

    invalid_in_silver = silver.filter(
        f"NOT ({valid_condition})"
    ).count()

    valid_in_quarantine = quarantine.filter(
        valid_condition
    ).count()

    assert invalid_in_silver == 0
    assert valid_in_quarantine == 0