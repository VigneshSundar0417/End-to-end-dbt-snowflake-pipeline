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
### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
