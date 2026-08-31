WITH silver AS (
    SELECT
        COUNT(*) AS row_count,
        COALESCE(SUM(quantity), 0) AS total_quantity,
        COALESCE(ROUND(SUM(price), 2), 0) AS total_amount
    FROM IDENTIFIER(:silver_orders_table)
),

gold AS (
    SELECT
        COUNT(*) AS row_count,
        COALESCE(SUM(quantity), 0) AS total_quantity,
        COALESCE(ROUND(SUM(line_amount), 2), 0) AS total_amount
    FROM IDENTIFIER(:gold_fact_orders_table)
)

SELECT
    silver.row_count AS silver_count,
    gold.row_count AS gold_count,

    silver.total_quantity AS silver_quantity,
    gold.total_quantity AS gold_quantity,

    silver.total_amount AS silver_amount,
    gold.total_amount AS gold_amount,

    CASE
        WHEN silver.row_count = gold.row_count
         AND silver.total_quantity = gold.total_quantity
         AND silver.total_amount = gold.total_amount
        THEN 'PASS'
        ELSE 'FAIL'
    END AS reconciliation_status

FROM silver
CROSS JOIN gold;