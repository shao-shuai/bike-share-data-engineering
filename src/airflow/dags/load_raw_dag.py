from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import CreateS3BucketOperator, UploadFileS3Operator

default_args = {
    'owner': 'Shuai',
    'start_date': datetime(2019, 10, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

raw_datalake_bucket_name = 'shuaishao-udacity'

dag = DAG('raw_datalake_dag',
          default_args=default_args,
          description='Load data into raw S3 datalake.',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_raw_datalake = CreateS3BucketOperator(
    task_id='Create_raw_datalake',
    bucket_name=raw_datalake_bucket_name,
    dag=dag
)

upload_trip_data = UploadFileS3Operator(
	task_id='upload_trip_data',
	bucket_name=raw_datalake_bucket_name,
	path='/opt/bitnami/dataset/trip_data',
	dag=dag
	)

upload_station_data = UploadFileS3Operator(
	task_id='upload_station_data',
	bucket_name=raw_datalake_bucket_name,
	path='/opt/bitnami/dataset/station_data',
	dag=dag
	)

upload_region_data = UploadFileS3Operator(
	task_id='upload_region_data',
	bucket_name=raw_datalake_bucket_name,
	path='/opt/bitnami/dataset/region_data',
	dag=dag
	)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# define dependency
start_operator >> create_raw_datalake
create_raw_datalake >> upload_trip_data
upload_trip_data >> end_operator