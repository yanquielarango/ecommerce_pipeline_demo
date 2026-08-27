from pyspark.sql import functions as F


def build_dim_date(df):
    return (
        df.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
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