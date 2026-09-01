import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821

SOURCE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/product_category_name/"

PRODUCT_CATEGORIES_SCHEMA = StructType(
    [
        StructField("product_category_name", StringType(), True),
        StructField("product_category_name_english", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


@dp.table(
    name=f"{BRONZE_SCHEMA}.brz_product_categories",
    comment="Raw product category translation data ingested from CSV files",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def product_categories_bronze():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(PRODUCT_CATEGORIES_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(SOURCE_PATH)
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("ingest_datetime", F.current_timestamp())
    )
