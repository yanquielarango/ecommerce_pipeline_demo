CREATE OR REPLACE VIEW dbr_dev.ecommerce_gold.vw_fact_orders_rls AS
SELECT
    f.*
FROM dbr_dev.ecommerce_gold.fact_orders f
INNER JOIN dbr_dev.ecommerce_gold.dim_customer c
    ON f.customer_key = c.customer_key
WHERE
    is_account_group_member('ecommerce_sp')
    AND c.customer_state = 'SP';