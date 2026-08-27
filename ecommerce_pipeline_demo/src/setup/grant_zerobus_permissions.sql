EXECUTE IMMEDIATE 'GRANT USE CATALOG ON CATALOG `' || {{catalog}} || '` TO `' || {{zerobus_principal_id}} || '`';

EXECUTE IMMEDIATE 'GRANT USE SCHEMA ON SCHEMA `' || {{catalog}} || '`.`' || {{bronze_schema}} || '` TO `' || {{zerobus_principal_id}} || '`';

EXECUTE IMMEDIATE 'GRANT MODIFY, SELECT ON TABLE `' || {{catalog}} || '`.`' || {{bronze_schema}} || '`.`brz_orders` TO `' || {{zerobus_principal_id}} || '`';