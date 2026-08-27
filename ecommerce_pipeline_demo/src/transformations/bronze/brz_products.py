from brz_common_functions import add_bronze_metadata
from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")  # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821

SOURCE_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/products/"

PRODUCTS_SCHEMA_HINTS = """
    product_id STRING,
    product_category_name STRING,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
"""


@dp.table(
    name=f"{BRONZE_SCHEMA}.brz_products",
    comment="Raw product data ingested from CSV files with schema evolution",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def products_bronze():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaHints", PRODUCTS_SCHEMA_HINTS)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(SOURCE_PATH)
        .transform(add_bronze_metadata)
    )