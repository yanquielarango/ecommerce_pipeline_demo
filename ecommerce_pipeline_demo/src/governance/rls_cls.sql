-- RLS
CREATE OR REPLACE VIEW dbr_dev.ecommerce_gold.vw_fact_orders_rls AS
SELECT
    f.*
FROM dbr_dev.ecommerce_gold.fact_orders f
INNER JOIN dbr_dev.ecommerce_gold.dim_customer c
    ON f.customer_key = c.customer_key
WHERE
    is_account_group_member('ecommerce_sp')
    AND c.customer_state = 'SP';

GRANT USE CATALOG ON CATALOG dbr_dev
TO `ecommerce_sp`;

GRANT USE SCHEMA ON SCHEMA dbr_dev.ecommerce_gold
TO `ecommerce_sp`;

GRANT SELECT ON VIEW dbr_dev.ecommerce_gold.vw_fact_orders_rls
TO `ecommerce_sp`;

REVOKE SELECT ON TABLE dbr_dev.ecommerce_gold.fact_orders
FROM `ecommerce_sp`;

REVOKE SELECT ON MATERIALIZED VIEW dbr_dev.ecommerce_gold.dim_customer
FROM `ecommerce_sp`;


-- CLS
CREATE OR REPLACE FUNCTION dbr_dev.ecommerce_gold.mask_customer_unique_id(
    customer_unique_id STRING
)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('ecommerce_sp')
        THEN customer_unique_id
    ELSE '***'
END;

ALTER MATERIALIZED VIEW dbr_dev.ecommerce_gold.dim_customer
ALTER COLUMN customer_unique_id
SET MASK dbr_dev.ecommerce_gold.mask_customer_unique_id;