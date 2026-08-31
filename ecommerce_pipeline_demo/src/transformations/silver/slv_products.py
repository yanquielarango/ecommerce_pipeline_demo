import pyspark.sql.functions as F
from pyspark import pipelines as dp
from slv_products_functions import prepare_products

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821

PRODUCTS_DQ_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
}



@dp.temporary_view(name="products_clean")
@dp.expect_all_or_drop(PRODUCTS_DQ_RULES)
def products_clean():
    return prepare_products(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.brz_products")  # noqa: F821
    )


dp.create_streaming_table(
    name=f"{SILVER_SCHEMA}.slv_products",
    comment="Clean product records with SCD Type 2 history",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)

dp.create_auto_cdc_flow(
    target=f"{SILVER_SCHEMA}.slv_products",
    source="products_clean",
    keys=["product_id"],
    sequence_by=F.col("ingest_datetime"),
    stored_as_scd_type=2,
    track_history_except_column_list=["file_name", "ingest_datetime"],
)