# 4. Data Lakes with Spark
## Project: Data Lake

### Scenario
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Requirements
1. AWS CLI
2. SSH client
3. KEY and SECRET for an AWS user with AdministratorAccess policy (added to `df.cfg`)

### ETL Pipeline
The EMR cluster is created with
```
./create-cluster.sh <cluster-name> <key-pair-name>
```
which prints a `ClusterId`.

The status of the cluster is checked with
```
./check-cluster-status.sh <cluster-id>
```
until `Cluster.Status.State` is `WAITING`.

The cluster's master node is SSH-ed into with
```
ssh -i <path-to-key-pair>.pem hadoop@<cluster-ip>
```
where the `cluster-ip` is the `Cluster.MasterPublicDnsName` in the output of the previous command.

A `dl.cfg` file is created on the master node with
```
vi dl.cfg
```
and then populated as
```
[AWS]
KEY=<access-key-id>
SECRET=<secret-access-key>

[S3]
OUTPUT_BUCKET=<bucket-in-s3-for-output-files>
```

A python file is created on the master node with
```
vi etl.py
```
and then the contents of `etl.py` are pasted into it.

Ensure spark is referring to python3 (see https://knowledge.udacity.com/questions/185634) with
``` 
sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
```

The script is run with
```
/usr/bin/spark-submit --master yarn etl.py
```

Once complete, the SSH session is terminated with
```
exit
```

The cluster is terminated with
```
./terminate-cluster.sh <cluster-id>
```

### Schema
#### Fact Table
songplays:
```
 |-- songplay_id: long (nullable = false)
 |-- start_time: timestamp (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```

#### Dimension Tables
artists:
```
 |-- artist_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- location: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
```

songs:
```
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: long (nullable = true)
 |-- duration: double (nullable = true)
```

time:
```
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- weekday: string (nullable = true)
```

users:
```
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
```
