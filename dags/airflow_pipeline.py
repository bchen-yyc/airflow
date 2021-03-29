from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, 
                               LoadFactOperator,
                               LoadDimensionOperator, 
                               DataQualityOperator)


from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
    'start_date': datetime(2019, 1, 12)
}

dag = DAG('airflow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          #start_date=datetime.utcnow()
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='public.staging_events',
    source_path='s3://udacity-dend/log_data',
    JSON_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='public.staging_songs',
    source_path='s3://udacity-dend/song_data',
    JSON_path='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    trunc_bf_insert=False,
    sql_query=SqlQueries.songplay_table_insert  
)

# Load dimension tables
dim_tables = ['songs','artists','time','users']
insert_queries = [SqlQueries.song_table_insert, 
                  SqlQueries.artist_table_insert, 
                  SqlQueries.time_table_insert, 
                  SqlQueries.user_table_insert]
load_dim_table_tasks = []

for i in range(len(dim_tables)):
    task = LoadDimensionOperator(
    task_id=f'Load_{dim_tables[i]}_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table=dim_tables[i],
    trunc_bf_insert=True,
    sql_query=insert_queries[i]
)
    load_dim_table_tasks.append(task)
    
tables = ['staging_events','staging_songs','songplays','songs','artists','time','users']
tables_checks = [{'check_sql': f'SELECT COUNT(*) FROM {table}', 
                  'expected_result': 0, 
                  'comparison': '>'} for table in tables]

dim_table_pkeys = [('songs','songid'), ('artists','artistid'), ('time','start_time'), ('users', 'userid')]
pkey_checks = [{'check_sql': f'SELECT COUNT(*) FROM {table} WHERE {pkey} is null', 
                'expected_result': 0, 
                'comparison': '='} for (table, pkey) in dim_table_pkeys]
dq_checks = tables_checks + pkey_checks

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dq_checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> load_dim_table_tasks >> run_quality_checks >> end_operator
