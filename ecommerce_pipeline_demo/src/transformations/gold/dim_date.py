from pyspark import pipelines as dp

from transformations.gold.dim_date_functions import build_dim_date

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
    },
)
def dim_date():
    dates = spark.sql(  # noqa: F821
        f"""
        SELECT explode(
            sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)
        ) AS full_date
        """
    )
    return build_dim_date(dates)