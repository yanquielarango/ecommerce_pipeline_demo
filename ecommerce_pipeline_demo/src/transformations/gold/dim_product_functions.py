from pyspark.sql import functions as F


def build_dim_product(products, categories):
    current_products = products.filter(F.col("__END_AT").isNull())

    return (
        current_products.alias("p")
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