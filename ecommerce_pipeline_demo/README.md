# Ecommerce Data Pipeline

End to end ecommerce data pipeline built with **Databricks**, **Lakeflow Declarative Pipelines**, **Databricks Zerobus**, **Azure DevOps** and **Power BI**.

Order events are sent to Databricks through Zerobus. The data is processed through Bronze, Silver and Gold layers and exposed to Power BI for reporting.

## Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Medallion Architecture](#medallion-architecture)
* [Technology Stack](#technology-stack)
* [Project Structure](#project-structure)
* [Prerequisites](#prerequisites)
* [Local Setup](#local-setup)
* [Environment Variables](#environment-variables)
* [Running the Producer](#running-the-producer)
* [Data Quality Testing](#data-quality-testing)
* [Producer CLI](#producer-cli)
* [Unit Tests](#unit-tests)
* [Reconciliation](#reconciliation)
* [Data Quality Alert](#data-quality-alert)
* [Row Level Security](#row-level-security)
* [Development and Production](#development-and-production)
* [CI/CD](#cicd)
* [Power BI](#power-bi)
* [Validation](#validation)
* [Troubleshooting](#troubleshooting)
* [Security](#security)

## Overview

```text
CSV Source Data
      │
      ▼
Python Order Producer
      │
      ▼
Databricks Zerobus
      │
      ▼
Bronze
      │
      ▼
Silver
      │
      ▼
Gold
      │
      ▼
Databricks SQL
      │
      ▼
Power BI
```

Lakeflow Declarative Pipelines handle the transformation layer and Unity Catalog is used for governance and access control.

Databricks Asset Bundles manage the project resources. Azure DevOps validates and deploys the bundle to development and production.

## Architecture

### Order Producer

The producer is written in Python and reads order and order item data from CSV files.

Each `order_id + product_id` combination becomes an order event and is sent directly to the Bronze table through Zerobus.

The producer supports finite and continuous execution.

### Finite Mode

Send a fixed number of events and stop.

```bash
uv run python src/producer/order_event_producer.py --num-events 250
```

### Continuous Mode

Keep sending events until the process is stopped manually.

```bash
uv run python src/producer/order_event_producer.py --continuous
```

Stop it with:

```text
Ctrl+C
```

The Zerobus stream is closed before the producer exits.

### Example Event

```json
{
  "order_id": "example-order-id",
  "customer_id": "example-customer-id",
  "product_id": "example-product-id",
  "quantity": 1,
  "price": 120.50,
  "order_timestamp": "2018-08-15T10:30:00+00:00",
  "discount_code": "WELCOME10",
  "ingest_datetime": "2026-08-24T09:30:00+00:00"
}
```

`order_timestamp` comes from the source order.

`ingest_datetime` is generated when the event is sent to Databricks.

## Medallion Architecture

### Bronze

Bronze receives the raw order events sent through Zerobus.

Development table:

```text
dbr_dev.ecommerce_bronze.brz_orders
```

The catalog and schema depend on the bundle target.

Batch source data for customers and products is also loaded through the Bronze layer.

### Silver

Silver contains cleaned and validated data.

The orders pipeline handles data quality checks and deduplication before records are used downstream.

Main order tables:

```text
slv_orders
quarantine_orders
```

Current order quality rules:

```text
order_id IS NOT NULL
customer_id IS NOT NULL
product_id IS NOT NULL
quantity IS NOT NULL AND quantity > 0
price IS NOT NULL AND price >= 0
```

Valid records continue to `slv_orders`.

Invalid records are stored in `quarantine_orders`.

Orders are deduplicated using:

```text
order_id + product_id
```

The customer transformation also keeps historical changes using SCD.

### Quarantine

Rejected order events are preserved instead of being silently dropped.

Examples:

```text
quantity = 0
quarantine_reason = quantity must be greater than 0
```

```text
price = -10
quarantine_reason = price must be greater than or equal to 0
```

```text
customer_id = null
quarantine_reason = customer_id is null
```

### Gold

Gold contains the star schema used for analytics.

Main tables:

```text
fact_orders
dim_date
dim_customer
dim_product
```

`fact_orders` is the central fact table.

```text
                 dim_date
                    │
                    │
dim_customer ─── fact_orders ─── dim_product
```

The core Silver and Gold transformation logic is kept separate from the Lakeflow resource definitions so it can also be tested locally.

## Technology Stack

| Area                  | Technology                     |
| --------------------- | ------------------------------ |
| Language              | Python                         |
| Dependency management | uv                             |
| Data platform         | Azure Databricks               |
| Streaming ingestion   | Databricks Zerobus             |
| Processing            | Apache Spark and PySpark       |
| Pipelines             | Lakeflow Declarative Pipelines |
| Storage               | Delta Lake                     |
| Governance            | Unity Catalog                  |
| Row level security    | Unity Catalog dynamic view     |
| Testing               | pytest and local PySpark       |
| Deployment            | Databricks Asset Bundles       |
| CI/CD                 | Azure DevOps                   |
| Analytics             | Databricks SQL                 |
| Reporting             | Power BI                       |

## Project Structure

```text
ecommerce_pipeline_demo/
│
├── data/
│   ├── customers/
│   ├── order_items/
│   ├── orders/
│   ├── product_category_name/
│   └── products/
│
├── env/
│   ├── dev.yml
│   ├── prod.yml
│   └── variables.yml
│
├── notebooks/
│   ├── quality/
│   │   └── orders_reconciliation.sql
│   └── security/
│       └── create_order_rls_views.sql
│
├── resources/
│   ├── alerts/
│   │   └── order_data_quality.yml
│   ├── jobs/
│   │   └── setup_bronze.job.yml
│   ├── pipelines/
│   │   └── pipeline.yml
│   ├── schemas/
│   │   ├── bronze.yml
│   │   ├── gold.yml
│   │   └── silver.yml
│   └── volumes/
│       └── landing.yml
│
├── src/
│   ├── producer/
│   │   └── order_event_producer.py
│   ├── setup/
│   │   ├── create_orders_bronze.sql
│   │   └── grant_zerobus_permissions.sql
│   └── transformations/
│       ├── bronze/
│       │   ├── customers.py
│       │   ├── product_categories.py
│       │   └── products.py
│       ├── silver/
│       │   ├── customers.py
│       │   ├── orders.py
│       │   ├── orders_transform.py
│       │   ├── product_categories.py
│       │   └── products.py
│       └── gold/
│           ├── dim_customer.py
│           ├── dim_date.py
│           ├── dim_product.py
│           ├── fact_orders.py
│           └── fact_orders_transform.py
│
├── tests/
│   ├── conftest.py
│   ├── test_gold_orders.py
│   └── test_silver_orders.py
│
├── .gitignore
├── azure-pipelines.yml
├── databricks.yml
├── pyproject.toml
├── requirements-test.txt
├── uv.lock
└── README.md
```

Local files and generated directories such as `.env`, `.venv`, `.venv-test`, `.databricks`, `.pytest_cache`, `.ruff_cache`, `.vscode` and `__pycache__` should not be committed.

## Prerequisites

You need:

```text
Python
uv
Git
Azure Databricks workspace access
Databricks Service Principal
Zerobus endpoint
```

Check the local tools with:

```bash
python --version
uv --version
git --version
```

## Local Setup

Clone the repository:

```bash
git clone <repository-url>
cd ecommerce_pipeline_demo
```

Install the normal project dependencies:

```bash
uv sync
```

## Environment Variables

The producer requires four environment variables:

```bash
export ZEROBUS_SERVER_ENDPOINT="<zerobus-server-endpoint>"
export DATABRICKS_WORKSPACE_URL="<databricks-workspace-url>"
export DATABRICKS_CLIENT_ID="<service-principal-client-id>"
export DATABRICKS_CLIENT_SECRET="<service-principal-client-secret>"
```

The target table is optional:

```bash
export ZEROBUS_TABLE_NAME="<catalog>.<schema>.<table>"
```

If it is not set the development table is used:

```text
dbr_dev.ecommerce_bronze.brz_orders
```

### Databricks Workspace URL

Open the Azure Databricks workspace in the browser.

A workspace URL looks similar to:

```text
https://adb-1234567890123456.7.azuredatabricks.net
```

If the browser URL contains additional parameters:

```text
https://adb-1234567890123456.7.azuredatabricks.net/?o=1234567890123456
```

use only:

```text
https://adb-1234567890123456.7.azuredatabricks.net
```

Configure it with:

```bash
export DATABRICKS_WORKSPACE_URL="https://adb-1234567890123456.7.azuredatabricks.net"
```

### Workspace ID

The workspace ID is the numeric value associated with the workspace.

Example:

```text
https://adb-1234567890123456.7.azuredatabricks.net/?o=1234567890123456
```

Workspace ID:

```text
1234567890123456
```

### Workspace Region

The region can be checked from the Databricks workspace information.

Example:

```text
eastus
```

### Zerobus Server Endpoint

For Azure Databricks the endpoint follows this format:

```text
https://<workspace-id>.zerobus.<region>.azuredatabricks.net
```

Example:

```text
https://1234567890123456.zerobus.eastus.azuredatabricks.net
```

Configure it with:

```bash
export ZEROBUS_SERVER_ENDPOINT="https://1234567890123456.zerobus.eastus.azuredatabricks.net"
```

The Zerobus endpoint and the workspace URL are different values.

### Service Principal

The producer uses a Databricks Service Principal.

Configure the client ID:

```bash
export DATABRICKS_CLIENT_ID="<service-principal-client-id>"
```

Configure the secret:

```bash
export DATABRICKS_CLIENT_SECRET="<service-principal-client-secret>"
```

The secret should never be committed to Git.

### Zerobus Permissions

The setup SQL is stored in:

```text
src/setup/create_orders_bronze.sql
src/setup/grant_zerobus_permissions.sql
```

The first script creates the Bronze table used by Zerobus.

The second grants the required permissions to the Service Principal.

### Local `.env`

The variables can also be stored in a local `.env`:

```text
ZEROBUS_SERVER_ENDPOINT=<zerobus-server-endpoint>
DATABRICKS_WORKSPACE_URL=<databricks-workspace-url>
DATABRICKS_CLIENT_ID=<service-principal-client-id>
DATABRICKS_CLIENT_SECRET=<service-principal-client-secret>
ZEROBUS_TABLE_NAME=dbr_dev.ecommerce_bronze.brz_orders
```

Load it with:

```bash
set -a
source .env
set +a
```

`.env` must stay in `.gitignore`.

## Running the Producer

Run the producer from the project root.

### Default

```bash
uv run python src/producer/order_event_producer.py
```

This sends 20 events.

### Fixed Number of Events

```bash
uv run python src/producer/order_event_producer.py --num-events 250
```

### Continuous Mode

```bash
uv run python src/producer/order_event_producer.py --continuous
```

With discount codes:

```bash
uv run python src/producer/order_event_producer.py \
  --continuous \
  --include-discount-code
```

### Custom Batch Configuration

```bash
uv run python src/producer/order_event_producer.py \
  --num-events 250 \
  --min-batch-size 1 \
  --max-batch-size 8 \
  --min-batch-delay 0.3 \
  --max-batch-delay 2.5 \
  --include-discount-code
```

## Data Quality Testing

The producer can inject one invalid event to test the Silver quality rules and quarantine flow.

### Quantity Equal to Zero

```bash
uv run python src/producer/order_event_producer.py \
  --num-events 1 \
  --invalid-event quantity-zero
```

### Negative Price

```bash
uv run python src/producer/order_event_producer.py \
  --num-events 1 \
  --invalid-event negative-price
```

### Missing Customer

```bash
uv run python src/producer/order_event_producer.py \
  --num-events 1 \
  --invalid-event missing-customer-id
```

Only one event is modified during the execution.

```text
Producer
   │
   ▼
Zerobus
   │
   ▼
Bronze
   │
   ▼
Quality Rules
   │
   ├── Valid ─────► slv_orders
   │
   └── Invalid ───► quarantine_orders
```

## Producer CLI

| Argument                  | Default                            | Description                 |
| ------------------------- | ---------------------------------- | --------------------------- |
| `--orders-path`           | `data/orders/orders.csv`           | Orders source file          |
| `--order-items-path`      | `data/order_items/order_items.csv` | Order items source file     |
| `--num-events`            | `20`                               | Number of events to send    |
| `--continuous`            | Disabled                           | Keep sending until `Ctrl+C` |
| `--min-batch-size`        | `1`                                | Minimum batch size          |
| `--max-batch-size`        | `10`                               | Maximum batch size          |
| `--min-batch-delay`       | `0.3`                              | Minimum delay in seconds    |
| `--max-batch-delay`       | `2.5`                              | Maximum delay in seconds    |
| `--include-discount-code` | Disabled                           | Add random discount codes   |
| `--invalid-event`         | Disabled                           | Inject one invalid event    |

`--num-events` and `--continuous` cannot be used together.

## Unit Tests

The core Silver and Gold transformations have local unit tests.

Current tests cover:

```text
Silver
  valid order passes quality rules
  invalid quantity is quarantined

Gold
  fact_orders keys and calculations
```

The normal development environment uses Databricks Connect.

Local Spark tests run in a separate virtual environment with PySpark.

Create it:

```bash
uv venv .venv-test
```

Activate it:

```bash
source .venv-test/bin/activate
```

Install the test dependencies:

```bash
uv pip install -r requirements-test.txt
```

Run the tests:

```bash
pytest -v
```

Current result:

```text
3 passed
```

`requirements-test.txt` contains:

```text
pytest
pyspark==3.5.0
```

This setup allows the core transformation tests to run from a clean checkout without requiring a Databricks workspace.

## Reconciliation

The order reconciliation query is stored in:

```text
notebooks/quality/orders_reconciliation.sql
```

It is executed manually and is not part of the Lakeflow pipeline.

The query compares Bronze records with the expected Silver and quarantine results.

Example:

```text
bronze_count              771
valid_before_dedup        768
invalid_expected            3
duplicates_removed          2
expected_silver_count     766
actual_silver_count       766
actual_quarantine_count     3
reconciliation_status    PASS
```

The check confirms that valid, invalid and duplicate records are accounted for correctly.

## Data Quality Alert

The data quality alert is stored in:

```text
resources/alerts/order_data_quality.yml
```

It checks `quarantine_orders` for recently rejected records.

Trigger condition:

```text
invalid_records > 0
```

The alert uses the SQL Warehouse configured through:

```text
sql_warehouse_id
```

It is deployed with the Databricks Asset Bundle.

## Row Level Security

Row level security is implemented in Unity Catalog using a secure view and a Databricks account group.

The SQL is stored in:

```text
notebooks/security/create_order_rls_views.sql
```

The current demo group is:

```text
ecommerce_sp
```

Members of this group can access orders associated with customers from São Paulo.

The secure view is:

```text
dbr_dev.ecommerce_gold.vw_fact_orders_rls
```

The RLS rule uses the group membership of the current user:

```sql
CREATE OR REPLACE VIEW dbr_dev.ecommerce_gold.vw_fact_orders_rls AS
SELECT
    f.*
FROM dbr_dev.ecommerce_gold.fact_orders f
INNER JOIN dbr_dev.ecommerce_gold.dim_customer c
    ON f.customer_key = c.customer_key
WHERE
    is_account_group_member('ecommerce_sp')
    AND c.customer_state = 'SP';
```

Check group membership:

```sql
SELECT
    session_user(),
    is_account_group_member('ecommerce_sp') AS is_sp_member;
```

For a member of `ecommerce_sp`:

```text
is_sp_member = true
```

The RLS implementation was validated with the original fact table:

```sql
SELECT COUNT(*) AS total_orders
FROM dbr_dev.ecommerce_gold.fact_orders;
```

Result:

```text
766
```

Secure view:

```sql
SELECT COUNT(*) AS visible_orders
FROM dbr_dev.ecommerce_gold.vw_fact_orders_rls;
```

Result:

```text
343
```

The visible customer state was also checked and returned only:

```text
SP
```

RLS is kept outside the Lakeflow pipeline. The pipeline creates the Gold objects and Unity Catalog handles the secure access layer.

## Development and Production

Environment configuration is stored in:

```text
env/dev.yml
env/prod.yml
env/variables.yml
```

Bundle variables include:

```text
catalog
bronze_schema
silver_schema
gold_schema
landing_storage_location
sql_warehouse_id
zerobus_principal_id
```

The same project can be deployed to development and production using different values.

## CI/CD

Azure DevOps validates and deploys the Databricks Asset Bundle.

### Development

A push to `main` triggers validation and deployment to development.

```text
Push to main
      │
      ▼
Azure DevOps
      │
      ├── Validate Bundle
      │
      └── Deploy to Development
```

### Production

Production deployment is triggered with a semantic version Git tag.

Example:

```bash
git tag v1.0.0
git push origin v1.0.0
```

```text
Git tag
   │
   ▼
Azure DevOps
   │
   ├── Validate Production Bundle
   │
   └── Deploy to Production
```

A normal push to `main` does not deploy to production.

## Power BI

Power BI connects to the Gold layer through a Databricks SQL Warehouse.

The reporting model follows a star schema:

```text
                 dim_date
                    │
                    │
dim_customer ─── fact_orders ─── dim_product
```

The model is used for metrics such as:

```text
Total orders
Revenue
Average order value
Sales by date
Customer activity
Product performance
Discount usage
```

The Date dimension is used for time based reporting.

`vw_fact_orders_rls` keeps the same fact keys as `fact_orders` and can be used as a secure fact source when Databricks side RLS is required.

## Validation

### Bronze

```sql
SELECT COUNT(*)
FROM <catalog>.<bronze_schema>.brz_orders;
```

Latest events:

```sql
SELECT *
FROM <catalog>.<bronze_schema>.brz_orders
ORDER BY ingest_datetime DESC
LIMIT 20;
```

### Silver

```sql
SELECT COUNT(*)
FROM <catalog>.<silver_schema>.slv_orders;
```

### Quarantine

```sql
SELECT *
FROM <catalog>.<silver_schema>.quarantine_orders
ORDER BY ingest_datetime DESC;
```

### Gold

```sql
SELECT COUNT(*)
FROM <catalog>.<gold_schema>.fact_orders;
```

### Reconciliation

Run:

```text
notebooks/quality/orders_reconciliation.sql
```

Expected result:

```text
PASS
```

### Unit Tests

Activate the test environment:

```bash
source .venv-test/bin/activate
```

Run:

```bash
pytest -v
```

Expected result:

```text
3 passed
```

### RLS

Check group membership:

```sql
SELECT
    is_account_group_member('ecommerce_sp') AS is_sp_member;
```

Check visible orders:

```sql
SELECT COUNT(*)
FROM dbr_dev.ecommerce_gold.vw_fact_orders_rls;
```

Check visible states:

```sql
SELECT DISTINCT
    c.customer_state
FROM dbr_dev.ecommerce_gold.vw_fact_orders_rls f
INNER JOIN dbr_dev.ecommerce_gold.dim_customer c
    ON f.customer_key = c.customer_key;
```

## Troubleshooting

### Missing Zerobus Endpoint

```text
RuntimeError: ZEROBUS_SERVER_ENDPOINT environment variable is required
```

Load the required variables before running the producer.

### Environment Variables Work in the Terminal but Not in the IDE

Configure the same variables in the IDE run configuration.

### Continuous Mode Reuses Orders

Continuous mode uses the complete source event pool.

When it reaches the end it reshuffles the events and starts another cycle.

This means the same `order_id + product_id` can appear again during long runs.

Silver handles duplicates using the order and product key.

### Requested Number of Events Is Too Large

If `--num-events` is greater than the available source event pool the producer stops with an error.

### PySpark Tests Start Databricks Connect

Run the local tests from `.venv-test`:

```bash
source .venv-test/bin/activate
pytest -v
```

The test environment uses local PySpark instead of Databricks Connect.

### RLS Returns No Rows

Check the account group membership:

```sql
SELECT
    session_user(),
    is_account_group_member('ecommerce_sp');
```

For the current rule the membership check must return `true`.

## Security

Do not store credentials in Python code.

Do not commit `.env`, `.venv`, `.venv-test`, client secrets or access tokens.

Production credentials should be stored as secure Azure DevOps variables.

The Zerobus Service Principal should only have the permissions required by the producer.

Gold row level security is handled in Unity Catalog using an account group and a secure view.
