import pyspark.sql.functions as F


def prepare_product_categories(df):
    return (
        df.select(
            F.trim(F.col("product_category_name")).alias("product_category_name"),
            F.trim(F.col("product_category_name_english")).alias("product_category_name_english"),
            "file_name",
            "ingest_datetime",
        ).dropDuplicates(["product_category_name"])
    )