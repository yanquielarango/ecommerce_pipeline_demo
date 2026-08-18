import re

import pyspark.sql.functions as F
from pyspark import pipelines as dp

BRONZE_SCHEMA = spark.conf.get("bronze_schema")  # noqa: F821
EVENTHUB_NAME = spark.conf.get("eventhub_name")  # noqa: F821

CONNECTION_STRING = dbutils.secrets.get(  # noqa: F821
    scope="e-commerce-bronze-scope",
    key="evh-brazilian-ecommerce",
)

NAMESPACE_MATCH = re.search(
    r"sb://([^./]+)\.servicebus\.windows\.net",
    CONNECTION_STRING,
)

if not NAMESPACE_MATCH:
    raise ValueError("Unable to extract Event Hub namespace")

NAMESPACE = NAMESPACE_MATCH.group(1)

BOOTSTRAP_SERVERS = f"{NAMESPACE}.servicebus.windows.net:9093"

SASL_CONFIG = (
    "kafkashaded.org.apache.kafka.common.security.plain."
    "PlainLoginModule required "
    f'username="$ConnectionString" '
    f'password="{CONNECTION_STRING}";'
)

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": SASL_CONFIG,
    "subscribe": EVENTHUB_NAME,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
}


@dp.table(
    name=f"{BRONZE_SCHEMA}.brz_orders",
    comment="Raw order events ingested from Azure Event Hub",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "eventhub",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def orders_bronze():
    raw_df = (
        spark.readStream  # noqa: F821
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    return (
        raw_df
        .select(
            F.col("value").cast("string").alias("json_payload"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "data",
            F.from_json(
                F.col("json_payload"),
                None,
                {
                    "schemaLocationKey": "orders_event_schema",
                    "schemaEvolutionMode": "addNewColumns",
                },
            ),
        )
        .select(
            "data.*",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .withColumn("source", F.lit(EVENTHUB_NAME))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )