from pyspark.sql import functions as F


def build_dim_customer(df):
    return (
        df.filter(F.col("__END_AT").isNull())
        .withColumn("customer_key", F.xxhash64("customer_id"))
        .withColumn("customer_state_location", F.concat_ws(", ", F.col("customer_state"), F.lit("Brazil")))
        .select(
            "customer_key",
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "customer_state_location",
        )
    )