from pyspark import pipelines as dp
from slv_product_categories_functions import prepare_product_categories

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.slv_product_categories",
    comment="Clean and validated product category translations",
    table_properties={
        "quality": "silver",
        "layer": "silver",
    },
)
@dp.expect_or_drop(
    "valid_product_category_name",
    "product_category_name IS NOT NULL AND product_category_name <> ''",
)
@dp.expect_or_drop(
    "valid_english_category_name",
    "product_category_name_english IS NOT NULL AND product_category_name_english <> ''",
)
def product_categories_silver():
    return prepare_product_categories(
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.brz_product_categories")  # noqa: F821
    )