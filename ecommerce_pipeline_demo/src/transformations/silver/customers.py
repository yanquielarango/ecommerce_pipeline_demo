import pyspark.sql.functions as F

from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821


@dp.temporary_view(
    name="customers_clean"
)
def customers_clean():
    return (
        spark.readStream.table(  # noqa: F821
            f"{CATALOG}.{BRONZE_SCHEMA}.brz_customers"
        )
        .filter(F.col("_corrupt_record").isNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("customer_unique_id").isNotNull())
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            F.trim(F.col("customer_city")).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
            "ingest_datetime",
        )
    )


dp.create_streaming_table(
    name=f"{SILVER_SCHEMA}.slv_customers"
)


dp.create_auto_cdc_flow(
    target=f"{SILVER_SCHEMA}.slv_customers",
    source="customers_clean",
    keys=["customer_id"],
    sequence_by=F.col("ingest_datetime"),
    except_column_list=["ingest_datetime"],
    stored_as_scd_type=2,
)