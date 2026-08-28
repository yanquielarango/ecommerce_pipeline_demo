import argparse
from pathlib import Path

import pyspark.sql.functions as F
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

CHECKS_DIR = Path(__file__).parent / "checks"


def parse_args():
    parser = argparse.ArgumentParser(description="Run DQX checks")

    parser.add_argument(
        "--table",
        required=True,
        help="Fully qualified table name",
    )
    parser.add_argument(
        "--checks",
        required=True,
        help="DQX YAML file name",
    )

    return parser.parse_args()


def run_dqx(spark, table_name, checks_file):
    engine = DQEngine(WorkspaceClient())

    checks = engine.load_checks_from_local_file(
        str(CHECKS_DIR / checks_file)
    )

    df = spark.table(table_name)

    result = engine.apply_checks_by_metadata(
        df,
        checks,
    )

    total_rows = result.count()

    error_rows = result.filter(
        F.size(F.col("_errors")) > 0
    ).count()

    warning_rows = result.filter(
        F.size(F.col("_warnings")) > 0
    ).count()

    print(f"Table: {table_name}")
    print(f"Rows checked: {total_rows}")
    print(f"Rows with errors: {error_rows}")
    print(f"Rows with warnings: {warning_rows}")

    if error_rows > 0:
        raise RuntimeError(
            f"DQX failed: {error_rows} rows contain data quality errors"
        )


def main():
    args = parse_args()

    spark = SparkSession.builder.getOrCreate()

    run_dqx(
        spark=spark,
        table_name=args.table,
        checks_file=args.checks,
    )


if __name__ == "__main__":
    main()