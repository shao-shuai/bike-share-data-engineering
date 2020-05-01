import airflow
import configparser
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from operators import CreateS3BucketOperator, UploadFileS3Operator
spark_script_bucket_name = 'shuaishao-spark-script'
raw_datalake_bucket_name = 'shuaishao-raw-datalake'
bikeshare_datalake_bucket_name = 'shuaishao-bikeshare-datalake'

default_args = {
    'owner': 'brfulu',
    'start_date': datetime(2019, 10, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

# SPARK_ETL_STEPS = [
#     {
#         'Name': 'Setup Debugging',
#         'ActionOnFailure': 'TERMINATE_CLUSTER',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['state-pusher-script']
#         }
#     },
#     {
#         'Name': 'Setup - copy files',
#         'ActionOnFailure': 'CANCEL_AND_WAIT',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['aws', 's3', 'cp', 's3://' + spark_script_bucket_name, '/home/hadoop/', '--recursive']
#         }
#     },
#     {
#         'Name': 'Airports - ETL',
#         'ActionOnFailure': 'CONTINUE',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['spark-submit', '/home/hadoop/script/airport_etl.py', 's3a://' + raw_datalake_bucket_name,
#                      's3a://' + accidents_datalake_bucket_name]
#         }
#     },
#     {
#         'Name': 'Cities - ETL',
#         'ActionOnFailure': 'CONTINUE',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['spark-submit', '/home/hadoop/script/city_etl.py', 's3a://' + raw_datalake_bucket_name,
#                      's3a://' + accidents_datalake_bucket_name]
#         }
#     },
#     {
#         'Name': 'Accidents - ETL',
#         'ActionOnFailure': 'CONTINUE',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['spark-submit', '/home/hadoop/script/accident_etl.py', 's3a://' + raw_datalake_bucket_name,
#                      's3a://' + accidents_datalake_bucket_name]
#         }
#     },
#     {
#         'Name': 'Check data quality',
#         'ActionOnFailure': 'CONTINUE',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['spark-submit', '/home/hadoop/script/check_data_quality.py',
#                      's3a://' + accidents_datalake_bucket_name]
#         }
#     }
# ]

JOB_FLOW_OVERRIDES = {
    'Name': 'Accidents-Datalake-ETL'
}

dag = DAG('bike_data_etl_dag',
          default_args=default_args,
          description='Extract transform and load data to S3 datalake.',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_code_bucket = CreateS3BucketOperator(
    task_id='Create_code_bucket',
    bucket_name=spark_script_bucket_name,
    dag=dag
)

upload_etl_code = UploadFileS3Operator(
    task_id='Upload_etl_code',
    bucket_name=spark_script_bucket_name,
    path='/opt/bitnami/script/',
    dag=dag
)

create_datalake_bucket = CreateS3BucketOperator(
    task_id='Create_datalake_bucket',
    bucket_name=bikeshare_datalake_bucket_name,
    dag=dag
)

create_cluster = EmrCreateJobFlowOperator(
    task_id='Create_EMR_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    dag=dag
)

