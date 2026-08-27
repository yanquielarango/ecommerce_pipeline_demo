CREATE TABLE IF NOT EXISTS IDENTIFIER({{catalog}} || '.' || {{bronze_schema}} || '.brz_orders') (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity BIGINT,
    price DOUBLE,
    order_timestamp STRING,
    discount_code STRING,
    ingest_datetime STRING
)
USING DELTA
TBLPROPERTIES (
    'quality' = 'bronze',
    'layer' = 'bronze',
    'source_format' = 'zerobus',
    'delta.enableChangeDataFeed' = 'true'
);