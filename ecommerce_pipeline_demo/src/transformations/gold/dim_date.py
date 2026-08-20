from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")  # noqa: F821
GOLD_SCHEMA = spark.conf.get("gold_schema")  # noqa: F821
START_DATE = spark.conf.get("start_date")  # noqa: F821
END_DATE = spark.conf.get("end_date")  # noqa: F821


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD_SCHEMA}.dim_date",
    comment="Date dimension for the Gold layer",
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def dim_date():
    df = spark.sql(  # noqa: F821
        f"""
        SELECT explode(
            sequence(
                to_date('{START_DATE}'),
                to_date('{END_DATE}'),
                interval 1 day
            )
        ) AS full_date
        """
    )

    return (
        df
        .withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
        .withColumn("day_of_week_num", F.dayofweek("full_date"))
        .withColumn("is_weekend", F.col("day_of_week_num").isin(1, 7))
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week_of_year",
            "day",
            "day_name",
            "is_weekend",
        )
    )