from dim_product_functions import build_dim_product
from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema")  # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_product",
    comment="Current product dimension for the Gold layer",
    table_properties={
        "quality": "gold",
        "layer": "gold",
    },
)
def dim_product():
    products = dp.read(f"{SILVER_SCHEMA}.slv_products") 
    categories = dp.read(f"{SILVER_SCHEMA}.slv_product_categories") 
    return build_dim_product(products, categories)