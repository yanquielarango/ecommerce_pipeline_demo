

  WITH bronze AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        price,
        ingest_datetime
    FROM dbr_dev.ecommerce_bronze.brz_orders
),

quality_checked AS (
    SELECT
        *,
        CASE
            WHEN order_id IS NOT NULL
             AND customer_id IS NOT NULL
             AND product_id IS NOT NULL
             AND quantity IS NOT NULL
             AND quantity > 0
             AND price IS NOT NULL
             AND price >= 0
            THEN TRUE
            ELSE FALSE
        END AS is_valid
    FROM bronze
),

valid_rows AS (
    SELECT *
    FROM quality_checked
    WHERE is_valid = TRUE
),

invalid_rows AS (
    SELECT *
    FROM quality_checked
    WHERE is_valid = FALSE
),

valid_unique_keys AS (
    SELECT DISTINCT
        order_id,
        product_id
    FROM valid_rows
),

bronze_count AS (
    SELECT COUNT(*) AS value
    FROM bronze
),

valid_count AS (
    SELECT COUNT(*) AS value
    FROM valid_rows
),

invalid_count AS (
    SELECT COUNT(*) AS value
    FROM invalid_rows
),

expected_silver_count AS (
    SELECT COUNT(*) AS value
    FROM valid_unique_keys
),

actual_silver_count AS (
    SELECT COUNT(*) AS value
    FROM dbr_dev.ecommerce_silver.slv_orders
),

actual_quarantine_count AS (
    SELECT COUNT(*) AS value
    FROM dbr_dev.ecommerce_silver.quarantine_orders
)

SELECT
    b.value AS bronze_count,

    v.value AS valid_before_dedup,

    i.value AS invalid_expected,

    v.value - es.value AS duplicates_removed,

    es.value AS expected_silver_count,

    s.value AS actual_silver_count,

    q.value AS actual_quarantine_count,

    CASE
        WHEN
            b.value = v.value + i.value
            AND es.value = s.value
            AND i.value = q.value
        THEN 'PASS'
        ELSE 'FAIL'
    END AS reconciliation_status

FROM bronze_count b
CROSS JOIN valid_count v
CROSS JOIN invalid_count i
CROSS JOIN expected_silver_count es
CROSS JOIN actual_silver_count s
CROSS JOIN actual_quarantine_count q;