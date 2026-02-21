# End-to-End ELT Pipeline Using Snowflake and dbt

## Architecture

- Data ingestion from source systems
- Storage in Snowflake warehouse
- Transformation using dbt models
- Data quality testing

## Technologies Used

- Snowflake Data Warehouse
- dbt (Data Build Tool)
- Python
- SQL
- AWS (optional ingestion)

## Pipeline Layers

1. RAW Layer – Source ingestion
2. STAGING Layer – Data cleaning
3. MART Layer – Business analytics

## Features

- Role-based access control
- Incremental transformations
- Automated testing
- Lineage visualization

## How to Run

```bash
dbt clean
dbt run
dbt test
