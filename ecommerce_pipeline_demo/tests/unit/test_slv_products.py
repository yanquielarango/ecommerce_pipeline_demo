from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformations.silver.slv_products_functions import prepare_products


PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_category_name", StringType(), True),
        StructField("product_name_lenght", IntegerType(), True),
        StructField("product_description_lenght", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("product_weight_g", IntegerType(), True),
        StructField("product_length_cm", IntegerType(), True),
        StructField("product_height_cm", IntegerType(), True),
        StructField("product_width_cm", IntegerType(), True),
        StructField("_rescued_data", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("ingest_datetime", TimestampType(), True),
    ]
)


def test_prepare_products_cleans_category_and_column_names(spark):
    input_df = spark.createDataFrame(
        [
            (
                "P001",
                "  furniture  ",
                10,
                20,
                3,
                500,
                10,
                20,
                30,
                None,
                "products.csv",
                None,
            )
        ],
        schema=PRODUCTS_SCHEMA,
    )

    result = prepare_products(input_df).collect()[0]

    assert result.product_category_name == "furniture"
    assert result.product_name_length == 10
    assert result.product_description_length == 20


def test_prepare_products_removes_rescued_records(spark):
    input_df = spark.createDataFrame(
        [
            (
                "P001",
                "furniture",
                10,
                20,
                3,
                500,
                10,
                20,
                30,
                None,
                "file1.csv",
                None,
            ),
            (
                "P002",
                "electronics",
                15,
                25,
                2,
                600,
                15,
                25,
                35,
                '{"unexpected":"value"}',
                "file2.csv",
                None,
            ),
        ],
        schema=PRODUCTS_SCHEMA,
    )

    result = prepare_products(input_df).collect()

    assert len(result) == 1
    assert result[0].product_id == "P001"