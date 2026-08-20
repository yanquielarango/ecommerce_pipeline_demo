import pyspark.sql.functions as F

from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.slv_product_categories",
    comment="Clean and validated product category translations",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop(
    "valid_product_category_name",
    "product_category_name IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_english_category_name",
    "product_category_name_english IS NOT NULL",
)
def product_categories_silver():
    return (
        spark.read.table(  # noqa: F821
            f"{CATALOG}.{BRONZE_SCHEMA}.brz_product_categories"
        )
        .select(
            F.trim(F.col("product_category_name")).alias(
                "product_category_name"
            ),
            F.trim(F.col("product_category_name_english")).alias(
                "product_category_name_english"
            ),
            "file_name",
            "ingest_datetime",
        )
        .dropDuplicates(["product_category_name"])
    )