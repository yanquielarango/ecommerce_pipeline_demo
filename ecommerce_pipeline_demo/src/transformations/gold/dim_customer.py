from dim_customer_functions import build_dim_customer
from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema")  # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_customer",
    comment="Current customer dimension for the Gold layer",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def dim_customer():
    return build_dim_customer(dp.read(f"{SILVER_SCHEMA}.slv_customers")) 