from pyspark.sql import functions as F


def build_dim_date(df):
    return (
        df.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("quarter_name", F.concat(F.lit("Q"), F.quarter("full_date")))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("month_year", F.date_format("full_date", "MMM yyyy"))
        .withColumn("month_year_sort", F.year("full_date") * 100 + F.month("full_date"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("day_of_week_num", F.dayofweek("full_date"))
        .withColumn("day_of_week_sort", F.pmod(F.col("day_of_week_num") - 2, F.lit(7)) + 1)
        .withColumn("is_weekend", F.col("day_of_week_num").isin(1, 7))
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "quarter_name",
            "month",
            "month_name",
            "month_year",
            "month_year_sort",
            "week_of_year",
            "day",
            "day_name",
            "day_of_week_sort",
            "is_weekend",
        )
    )