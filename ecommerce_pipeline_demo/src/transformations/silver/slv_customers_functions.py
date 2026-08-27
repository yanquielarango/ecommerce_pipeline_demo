import pyspark.sql.functions as F


def prepare_customers(df):
    return (
        df.filter(F.col("_corrupt_record").isNull())
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            F.trim(F.col("customer_city")).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
            "ingest_datetime",
        )
    )