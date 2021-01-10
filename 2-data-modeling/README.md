# 2. Data Modeling
## Project: Data Modeling with Cassandra

### Scenario
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions.

### Requirements (on local machine)
1. Python 3
2. Apache Cassandra (running)

### Installation
The Python dependencies are installed with:
```
pip install -r requirements.txt
```

### Running locally
The Cassandra tables are created by running:
```
python create_tables.py
```

The Cassandra tables are populated by running:
```
python etl.py
```

The data in the Cassandra tables are queried (and the results printed) by running:
```
python analyze.py
```

### Explanation of files/directories

`csql_queries.py`:
Contains the `CREATE`, `INSERT`, `DROP` and `SELECT` CSQL queries for all tables as strings.

`create_tables.py`:
Connects to the Cassandra database, and then drops and creates the three tables (also defined by their `CREATE` queries in `csql_queries.py`) in the `sparkify` keyspace.

`etl.py`:
Extracts, transforms and loads data from `event_data` CSV files into tables (defined by their `CREATE` queries in `csql_queries.py`) in the `sparkify` Cassandra keyspace.

`analyze.py`:
Executes queries on the data in the three tables in the `sparkify` Cassandra keyspace.

`project.ipynb`
A *Jupyter* notebook containing guidance and the basic framework for `create_tables.py`, `etl.py` and `analyze.py`.

`event_data`
A directory containing the raw CSV files that are ingested into Cassandra.
