from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from subdags import load_dimensional_tables_subdag
from helpers import SqlQueries

start_date = datetime.now()
default_args = {
    'owner': 'ollyfreeman',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

## Create top-level DAG
dag_name='sparkify_dag'
dag = DAG(
    'sparkify_dag',
    default_args=default_args,
    description='Extract Load and Transform data from S3 to Redshift',
    schedule_interval='@hourly',
)

## Create start operator
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

## Create operators for populating staging tables
# - staging_events table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    format_as='s3://udacity-dend/log_json_path.json',
    provide_context=True,
)

# - staging_songs table
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    format_as='auto',
    provide_context=True,
)

## Create operator for loading fact table
# - songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    sql_select_query=SqlQueries.songplay_table_insert,
)

## Create operators for loading dimension tables
# - users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table='users',
    sql_select_query=SqlQueries.user_table_insert,
)

# - songs table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    sql_select_query=SqlQueries.song_table_insert,
)

# - artists table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    sql_select_query=SqlQueries.artist_table_insert,
)

# - time table
# n.b. the use of a subdag here is to demonstrate how it works
# it adds no actual benefit in this scenario
load_time_dimension_table_task_id='Load_time_dim_table'
load_time_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_subdag(
        parent_dag_name=dag_name,
        task_id=load_time_dimension_table_task_id,
        table='time',
        sql_select_query=SqlQueries.time_table_insert,
        start_date=start_date,
    ),
    task_id=load_time_dimension_table_task_id,
    dag=dag,
)

## Create operator for data quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tests = [
        {
            'sql': 'SELECT COUNT(*) FROM users',
            'operator': '>',
            'value': 0,
        }, {
            'sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL',
            'operator': '==',
            'value': 0,
        }
    ]
)

## Create end operator
end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag,
)

# Specify DAG ordering
start_operator >> [
    stage_events_to_redshift,
    stage_songs_to_redshift,
]

[
    stage_events_to_redshift,
    stage_songs_to_redshift,
] >> load_songplays_table

load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
] >> run_quality_checks

run_quality_checks >> end_operator
