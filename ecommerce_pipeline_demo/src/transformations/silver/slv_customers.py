import pyspark.sql.functions as F
from pyspark import pipelines as dp
from slv_customers_functions import prepare_customers

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
SILVER_SCHEMA = spark.conf.get("silver_schema")  # noqa: F821

CUSTOMERS_DQ_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_customer_unique_id": "customer_unique_id IS NOT NULL",
}



@dp.expect_all_or_drop(CUSTOMERS_DQ_RULES)
@dp.temporary_view(name="customers_clean")
def customers_clean():
    return prepare_customers(
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.brz_customers")  # noqa: F821
    )


dp.create_streaming_table(name=f"{SILVER_SCHEMA}.slv_customers")

dp.create_auto_cdc_flow(
    target=f"{SILVER_SCHEMA}.slv_customers",
    source="customers_clean",
    keys=["customer_id"],
    sequence_by=F.col("ingest_datetime"),
    except_column_list=["ingest_datetime"],
    stored_as_scd_type=2,
)