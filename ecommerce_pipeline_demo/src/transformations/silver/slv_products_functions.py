import pyspark.sql.functions as F


def prepare_products(df):
    return (
        df.filter(F.col("_rescued_data").isNull())
        .select(
            "product_id",
            F.when(
                F.trim(F.col("product_category_name")) == "",
                None,
            )
            .otherwise(F.trim(F.col("product_category_name")))
            .alias("product_category_name"),
            F.col("product_name_lenght").alias("product_name_length"),
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