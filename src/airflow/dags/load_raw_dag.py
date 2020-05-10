from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import CreateS3BucketOperator, UploadFileS3Operator, CheckS3FileCount

default_args = {
    'owner': 'Shuai',
    'start_date': datetime(2019, 10, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

spark_script_bucket_name = 'shuaishao-spark-script'
raw_datalake_bucket_name = 'shuaishao-raw-datalake'
bikeshare_datalake_bucket_name = 'shuaishao-bikeshare-datalake'

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

upload_weather_data = UploadFileS3Operator(
	task_id='upload_weather_data',
	bucket_name=raw_datalake_bucket_name,
	path='/opt/bitnami/dataset/weather_data',
	dag=dag
	)

check_data_quality = CheckS3FileCount(
    task_id='check_data_quality',
    bucket_name=raw_datalake_bucket_name,
    expected_count=18,
    dag=dag
)

create_code_bucket = CreateS3BucketOperator(
    task_id='Create_code_bucket',
    bucket_name=spark_script_bucket_name,
    dag=dag
)

create_datalake_bucket = CreateS3BucketOperator(
    task_id='Create_datalake_bucket',
    bucket_name=bikeshare_datalake_bucket_name,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Define dependency
start_operator >> create_raw_datalake
create_raw_datalake >> upload_trip_data
create_raw_datalake >> upload_station_data
create_raw_datalake >> upload_region_data
create_raw_datalake >> upload_weather_data
upload_trip_data >> check_data_quality
upload_station_data >> check_data_quality
upload_region_data >> check_data_quality
upload_weather_data >> check_data_quality

check_data_quality >> create_code_bucket
check_data_quality >> create_datalake_bucket

create_code_bucket >> end_operator
create_datalake_bucket >> end_operator
