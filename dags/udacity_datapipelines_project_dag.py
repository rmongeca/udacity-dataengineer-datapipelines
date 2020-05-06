from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import StageToRedshiftOperator, \
    LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from datetime import datetime
from helpers import SqlQueries

default_args = {
    "owner": "udacity-student",
    "start_date": datetime(2018, 11, 1),
    "end_date": datetime(2018, 11, 1)
}

dag = DAG(
    "test_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@monthly"
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='End_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="Create_tables", dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_logs = StageToRedshiftOperator(
    task_id="Stage_events", dag=dag,
    s3_bucket="udacity-dend", s3_key="log_data", jsonpath="log_json_path.json",
    aws_credentials_id="aws_credentials", redshift_conn_id="redshift",
    table="staging_events"
)

stage_songs = StageToRedshiftOperator(
    task_id="Stage_songs", dag=dag,
    s3_bucket="udacity-dend", s3_key="song_data",
    aws_credentials_id="aws_credentials", redshift_conn_id="redshift",
    table="staging_songs"
)

load_fact = LoadFactOperator(
    task_id="Load_songplays_fact_table", dag=dag,
    table="songplays", subquery=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift"
)

load_dim_song = LoadDimensionOperator(
    task_id="Load_song_dim_table", dag=dag,
    table="songs", subquery=SqlQueries.song_table_insert,
    redshift_conn_id="redshift"
)

load_dim_artist = LoadDimensionOperator(
    task_id="Load_artist_dim_table", dag=dag,
    table="artists", subquery=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift"
)

load_dim_user = LoadDimensionOperator(
    task_id="Load_user_dim_table", dag=dag,
    table="users", subquery=SqlQueries.user_table_insert,
    redshift_conn_id="redshift"
)

load_dim_time = LoadDimensionOperator(
    task_id="Load_time_dim_table", dag=dag,
    table="time", subquery=SqlQueries.time_table_insert,
    redshift_conn_id="redshift"
)

data_quality = DataQualityOperator(
    task_id="Run_data_quality_checks", dag=dag,
    query="SELECT count(1) FROM songplays WHERE userId IS NULL",
    condition="len(records) > 0",
    redshift_conn_id="redshift"
)

start_operator >> create_tables

create_tables >> stage_logs
create_tables >> stage_songs

stage_logs >> load_fact
stage_songs >> load_fact

load_fact >> load_dim_song
load_fact >> load_dim_artist
load_fact >> load_dim_user
load_fact >> load_dim_time

load_dim_song >> data_quality
load_dim_artist >> data_quality
load_dim_user >> data_quality
load_dim_time >> data_quality

data_quality >> end_operator
