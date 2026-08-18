from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821

SOURCE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/customers/"

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", IntegerType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


@dp.table(
    name=f"{BRONZE_SCHEMA}.brz_customers",
    comment="Raw customer data ingested from CSV files",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def customers_bronze():
    return (
        spark.readStream  # noqa: F821
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(CUSTOMERS_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(SOURCE_PATH)
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("ingest_datetime", F.current_timestamp())
    )