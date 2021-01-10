# 3. Cloud Data Warehouses
## Project: Data Warehouse

### Scenario
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

### Requirements
1. Python 3
2. KEY and SECRET for an AWS user with AdministratorAccess policy (added to `dwh.cfg`)

### Installation
The Python dependencies are installed with:
```
pip install -r requirements.txt
```

### Running locally
The Redis cluster is created by running:
```
python create_cluster.py
```
The console output should then be used to update `CLUSTER.HOST` and `IAM_ROLE.ARN` in `dwh.cfg`.

The Redis database tables are created by running:
```
python create_tables.py
```

The Redis database tables are populated by running:
```
python etl.py
```

The Redis cluster is deleted by running:
```
python delete_cluster.py
```

### Explanation of files/directories

`create_cluster.py`:
Creates a Redis cluster with a database.

`create_tables.py`:
Connects to the Redis database, and then drops and creates the staging and regular tables (defined by their `CREATE` queries in `sql_queries.py`).

`delete_cluster.py`:
Destroys the Redis cluster.

`dwh.cfg`
A configuration file, including some parameters that should be filled out before/during execution of the scripts.

`etl.py`:
Loads data into the staging tables from S3, and then inserts data from the staging tables into the regular tables.

`sql_queries.py`:
Contains the `CREATE`, `INSERT`, `COPY`, `DROP` and `SELECT` SQL queries (as strings) for all tables.

`utils.py`:
Contains utility functions.
