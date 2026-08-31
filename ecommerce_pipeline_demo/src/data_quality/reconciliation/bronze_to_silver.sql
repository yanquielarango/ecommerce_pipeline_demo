WITH bronze AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        price
    FROM IDENTIFIER(:bronze_orders_table)
),

valid_rows AS (
    SELECT *
    FROM bronze
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND product_id IS NOT NULL
      AND quantity IS NOT NULL
      AND quantity > 0
      AND price IS NOT NULL
      AND price >= 0
),

invalid_rows AS (
    SELECT *
    FROM bronze
    WHERE order_id IS NULL
       OR customer_id IS NULL
       OR product_id IS NULL
       OR quantity IS NULL
       OR quantity <= 0
       OR price IS NULL
       OR price < 0
),

expected_silver AS (
    SELECT DISTINCT
        order_id,
        product_id
    FROM valid_rows
),

silver_count AS (
    SELECT COUNT(*) AS value
    FROM IDENTIFIER(:silver_orders_table)
),

quarantine_count AS (
    SELECT COUNT(*) AS value
    FROM IDENTIFIER(:quarantine_orders_table)
)

SELECT
    (SELECT COUNT(*) FROM bronze) AS bronze_count,
    (SELECT COUNT(*) FROM valid_rows) AS valid_count,
    (SELECT COUNT(*) FROM invalid_rows) AS invalid_count,
    (SELECT COUNT(*) FROM expected_silver) AS expected_silver_count,

    silver_count.value AS actual_silver_count,
    quarantine_count.value AS actual_quarantine_count,

    CASE
        WHEN (SELECT COUNT(*) FROM expected_silver) = silver_count.value
         AND (SELECT COUNT(*) FROM invalid_rows) = quarantine_count.value
        THEN 'PASS'
        ELSE 'FAIL'
    END AS reconciliation_status

FROM silver_count
CROSS JOIN quarantine_count;