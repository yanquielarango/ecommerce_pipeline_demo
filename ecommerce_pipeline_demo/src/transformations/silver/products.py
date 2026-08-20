import pyspark.sql.functions as F

from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.temporary_view(
    name="products_clean"
)
def products_clean():
    return (
        spark.readStream.table(  # noqa: F821
            f"{CATALOG}.{BRONZE_SCHEMA}.brz_products"
        )
        .filter(F.col("_corrupt_record").isNull())
        .filter(F.col("product_id").isNotNull())
        .select(
            "product_id",
            F.when(
                F.trim(F.col("product_category_name")) == "",
                None,
            )
            .otherwise(
                F.trim(F.col("product_category_name"))
            )
            .alias("product_category_name"),
            F.col("product_name_lenght").alias(
                "product_name_length"
            ),
            F.col("product_description_lenght").alias(
                "product_description_length"
            ),
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "file_name",
            "ingest_datetime",
        )
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
    track_history_except_column_list=[
        "file_name",
        "ingest_datetime",
    ],
)