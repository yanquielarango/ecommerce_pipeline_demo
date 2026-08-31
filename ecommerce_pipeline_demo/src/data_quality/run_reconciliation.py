import argparse
import os
from pathlib import Path

from databricks.connect import DatabricksSession

RECONCILIATION_DIR = Path(__file__).parent / "reconciliation"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run reconciliation checks"
    )

    parser.add_argument(
        "--query",
        required=True,
        help="Reconciliation SQL file name",
    )

    return parser.parse_args()


def run_reconciliation(spark, query_file: str):
    query_path = RECONCILIATION_DIR / query_file

    if not query_path.exists():
        raise FileNotFoundError(
            f"Reconciliation query not found: {query_path}"
        )

    sql = query_path.read_text(encoding="utf-8")

    catalog = os.environ["DATABRICKS_CATALOG"]
    bronze_schema = os.environ["DATABRICKS_BRONZE_SCHEMA"]
    silver_schema = os.environ["DATABRICKS_SILVER_SCHEMA"]
    gold_schema = os.environ["DATABRICKS_GOLD_SCHEMA"]

    if query_file == "bronze_to_silver.sql":
        query_args = {
            "bronze_orders_table": (
                f"{catalog}.{bronze_schema}.brz_orders"
            ),
            "silver_orders_table": (
                f"{catalog}.{silver_schema}.slv_orders"
            ),
            "quarantine_orders_table": (
                f"{catalog}.{silver_schema}.quarantine_orders"
            ),
        }

    elif query_file == "silver_to_gold.sql":
        query_args = {
            "silver_orders_table": (
                f"{catalog}.{silver_schema}.slv_orders"
            ),
            "gold_fact_orders_table": (
                f"{catalog}.{gold_schema}.fact_orders"
            ),
        }

    else:
        raise ValueError(
            f"Unsupported reconciliation query: {query_file}"
        )

    result = spark.sql(
        sql,
        args=query_args,
    ).collect()[0]

    print(f"\nReconciliation: {query_file}")

    for column, value in result.asDict().items():
        print(f"{column}: {value}")

    if result.reconciliation_status != "PASS":
        raise RuntimeError(
            f"Reconciliation failed: {query_file}"
        )


def main():
    args = parse_args()

    spark = (
        DatabricksSession.builder
        .serverless()
        .getOrCreate()
    )

    run_reconciliation(
        spark=spark,
        query_file=args.query,
    )


if __name__ == "__main__":
    main()