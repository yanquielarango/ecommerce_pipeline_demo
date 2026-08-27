import pyspark.sql.functions as F


def add_bronze_metadata(df):
    return (
        df.withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("ingest_datetime", F.current_timestamp())
    )