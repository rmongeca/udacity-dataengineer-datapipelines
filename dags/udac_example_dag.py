from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
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
    'udacity_data_pipeline_project',
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@monthly"
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket="udacity-dend", s3_key="log_data", jsonpath="log_json_path.json",
    aws_credentials_id="aws_credentials", redshift_conn_id="redshift",
    table="staging_events"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket="udacity-dend", s3_key="song_data",
    aws_credentials_id="aws_credentials", redshift_conn_id="redshift",
    table="staging_songs"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays", subquery=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users", subquery=SqlQueries.user_table_insert,
    redshift_conn_id="redshift"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs", subquery=SqlQueries.song_table_insert,
    redshift_conn_id="redshift"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists", subquery=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time", subquery=SqlQueries.time_table_insert,
    redshift_conn_id="redshift"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    query="SELECT count(1) FROM songplays WHERE userId IS NULL",
    condition="len(records) > 0",
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
