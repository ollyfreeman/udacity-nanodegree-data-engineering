# 4. Data Pipelines with Airflow
## Project: Data Lake

### Scenario
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Project structure
- `README.md`: Project overview
- `create_tables.sql`: SQL queries to create tables in Redshift (provided in project template)
- `dags/`
  - `dag.py`: Top-level DAG
  - `subdags.py`: SubDAGs of top-level DAG
- `plugins/`
  - `helpers/`
    - `__init__.py`: Init file for directory (provided in project template)
    - `sql_queries.py`: SQL queries to insert data into tables in Redshift (provided in project template)
  - `operators/`
    - `__init__.py`: Init file for directory (provided in project template)
    - `data_quality.py`: DataQualityOperator operator class
    - `load_dimension.py`: LoadDimensionOperator operator class
    - `load_fact.py`: LoadFactOperator operator class
    - `stage_redshift.py`: StageToRedshiftOperator operator class
  - `__init__.py`: Init file for directory (provided in project template)

### Requirements
1. Redshift cluster with public access
2. Airflow server (with this code uploaded, initialized with `/opt/airflow/start.sh`)

### ETL Pipeline
In Redshift, run the SQL queries in `create_tables.sql` to create all tables.

In Airflow, create a connection of type `Amazon Web Services` called `aws_credentials` with the credentials of any AWS user (since any user can access the public data sources).

In Airflow, create a connection of type `PostgreSQl` called `redshift` with the connection details of the Redshift cluster.

In Airflow, run the pipeline by toggling the DAG on, and then pressing the TRIGGER DAG button.
