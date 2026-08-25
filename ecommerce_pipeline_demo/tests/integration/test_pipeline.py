import pytest
from pyspark.sql import DataFrame, SparkSession


def _assert_no_invalid_rows(invalid_rows: DataFrame) -> None:
    """Fail with the actual bad-row count instead of a bare '0 != N'."""
    count = invalid_rows.count()
    assert count == 0, f"Found {count} invalid row(s)"


@pytest.mark.integration_test
def test_silver_orders_table_is_readable(
    spark: SparkSession,
    silver_orders_table: str,
):
    orders = spark.table(silver_orders_table)

    assert orders.count() > 0


@pytest.mark.integration_test
def test_silver_orders_contains_only_valid_orders(
    spark: SparkSession,
    silver_orders_table: str,
):
    invalid_orders = spark.sql(
        f"""
        SELECT *
        FROM {silver_orders_table}
        WHERE
            order_id IS NULL
            OR customer_id IS NULL
            OR product_id IS NULL
            OR quantity IS NULL
            OR quantity <= 0
            OR price IS NULL
            OR price < 0
        """
    )

    _assert_no_invalid_rows(invalid_orders)


@pytest.mark.integration_test
def test_fact_orders_contains_valid_business_values(
    spark: SparkSession,
    gold_fact_orders_table: str,
):
    invalid_rows = spark.sql(
        f"""
        SELECT *
        FROM {gold_fact_orders_table}
        WHERE
            order_id IS NULL
            OR date_key IS NULL
            OR quantity <= 0
            OR unit_price IS NULL
            OR unit_price < 0
            OR line_amount < 0
        """
    )

    _assert_no_invalid_rows(invalid_rows)