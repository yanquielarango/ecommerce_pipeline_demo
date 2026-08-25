from pyspark import pipelines as dp

from transformations.gold.fact_orders_functions import (
    build_fact_orders,
)


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
    orders = dp.read(
        f"{SILVER_SCHEMA}.slv_orders"
    )

    customers = dp.read(
        f"{GOLD_SCHEMA}.dim_customer"
    )

    products = dp.read(
        f"{GOLD_SCHEMA}.dim_product"
    )

    dates = dp.read(
        f"{GOLD_SCHEMA}.dim_date"
    )

    return build_fact_orders(
        orders=orders,
        customers=customers,
        products=products,
        dates=dates,
    )