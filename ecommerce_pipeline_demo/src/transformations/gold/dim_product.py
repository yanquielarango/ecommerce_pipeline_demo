from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema")  # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_product",
    comment="Current product dimension for the Gold layer",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def dim_product():
    products = dp.read(f"{SILVER_SCHEMA}.slv_products").filter(F.col("__END_AT").isNull())
    categories = dp.read(f"{SILVER_SCHEMA}.slv_product_categories")

    return (
        products.alias("p")
        .join(
            categories.alias("c"),
            F.col("p.product_category_name") == F.col("c.product_category_name"),
            "left",
        )
        .withColumn("product_key", F.xxhash64("p.product_id"))
        .select(
            "product_key",
            F.col("p.product_id").alias("product_id"),
            F.col("p.product_category_name").alias("product_category_name"),
            F.col("c.product_category_name_english").alias("product_category_name_english"),
            F.col("p.product_name_length").alias("product_name_length"),
            F.col("p.product_description_length").alias("product_description_length"),
            F.col("p.product_photos_qty").alias("product_photos_qty"),
            F.col("p.product_weight_g").alias("product_weight_g"),
            F.col("p.product_length_cm").alias("product_length_cm"),
            F.col("p.product_height_cm").alias("product_height_cm"),
            F.col("p.product_width_cm").alias("product_width_cm"),
        )
    )