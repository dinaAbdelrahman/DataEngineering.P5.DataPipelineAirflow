from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, CreateTableOperator)
from helpers import (SqlQueries, CREATE_ALL_TABLES_SQL)


AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Dina Abdel Rahman',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          ##https://medium.com/intage-analytics/airflow-trick-to-find-the-exact-start-date-via-cron-expression-23b5351007b
          schedule_interval='0 * * * *', ## Dag is running hourly
          max_active_runs=1,
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTableOperator(
    task_id='create_tables',
    postgres_conn_id='redshift',
    provide_context=True,
    dag = dag,
    sql_statment1 = CREATE_ALL_TABLES_SQL.CREATE_TABLE_artists,
    sql_statment2 = CREATE_ALL_TABLES_SQL.CREATE_TABLE_songplays,
    sql_statment3 = CREATE_ALL_TABLES_SQL.CREATE_TABLE_songs,
    sql_statment4 = CREATE_ALL_TABLES_SQL.CREATE_TABLE_staging_events,
    sql_statment5 = CREATE_ALL_TABLES_SQL.CREATE_TABLE_time
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend/',
    s3_key='log_data/{execution_date.year}/{execution_date.month}',
    json_path='s3://udacity-dend/log_json_path.json',
    region='us-west-2'
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend/',
    s3_key='song_data',
    json_path='auto',
    region='us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statment=SqlQueries.songplay_table_insert,
    table='songplays',
    mode='append-only'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statment=SqlQueries.user_table_insert,
    table='users',
    mode='append-only'    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statment=SqlQueries.song_table_insert,
    table='songs',
    mode='append-only'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statment=SqlQueries.artist_table_insert,
    table='artists',
    mode='append-only'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statment=SqlQueries.time_table_insert,
    table='time',
    mode='append-only'
    
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',  
    tables=['songplay', 'users', 'songs', 'artists', 'time'],
    min_records=1,
    sql_check="SELECT COUNT(*) FROM"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables

create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

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
