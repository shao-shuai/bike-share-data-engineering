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
    'owner': 'Shuai',
    'start_date': datetime(2019, 10, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

SPARK_ETL_STEPS = [
    {
        'Name': 'Setup Debugging',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    },
    {
        'Name': 'Setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', 's3://' + spark_script_bucket_name, '/home/hadoop/', '--recursive']
        }
    },
    {
        'Name': 'Trips - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/trip_etl.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + bikeshare_datalake_bucket_name]
        }
    },
    {
        'Name': 'Stations - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/station_etl.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + bikeshare_datalake_bucket_name]
        }
    },
    {
        'Name': 'Weather - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/weather_etl.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + bikeshare_datalake_bucket_name]
        }
    },
    {
        'Name': 'Regions - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/region_etl.py', 's3a://' + raw_datalake_bucket_name,
                     's3a://' + bikeshare_datalake_bucket_name]
        }
    },
    {
        'Name': 'Check data quality',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/check_data_quality.py',
                     's3a://' + bikeshare_datalake_bucket_name]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Bikeshare-Datalake-ETL'
}

dag = DAG('bike_data_etl_dag',
          default_args=default_args,
          description='Extract transform and load data to S3 datalake.',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# create_code_bucket = CreateS3BucketOperator(
#     task_id='Create_code_bucket',
#     bucket_name=spark_script_bucket_name,
#     dag=dag
# )

upload_etl_code = UploadFileS3Operator(
    task_id='Upload_etl_code',
    bucket_name=spark_script_bucket_name,
    path='/opt/bitnami/script/',
    dag=dag
)

# create_datalake_bucket = CreateS3BucketOperator(
#     task_id='Create_datalake_bucket',
#     bucket_name=bikeshare_datalake_bucket_name,
#     dag=dag
# )

create_cluster = EmrCreateJobFlowOperator(
    task_id='Create_EMR_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    region_name='us-west-2',
    dag=dag
)

add_jobflow_steps = EmrAddStepsOperator(
    task_id='Add_jobflow_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    region_name='us-west-2',
    steps=SPARK_ETL_STEPS,
    dag=dag
)

trip_processing = EmrStepSensor(
    task_id='trip_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[2] }}",
    aws_conn_id='aws_credentials',
    region_name='us-west-2',
    dag=dag
    )

station_processing = EmrStepSensor(
    task_id='station_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[3] }}",
    aws_conn_id='aws_credentials',
    region_name='us-west-2',
    dag=dag
    )

weather_processing = EmrStepSensor(
    task_id='weather_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[4] }}",
    aws_conn_id='aws_credentials',
    region_name='us-west-2',
    dag=dag
    )

region_processing = EmrStepSensor(
    task_id='region_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[5] }}",
    aws_conn_id='aws_credentials',
    region_name='us-west-2',
    dag=dag
    )

data_quality_check = EmrStepSensor(
    task_id='data_quality_check_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[6] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Define dependency
start_operator >> upload_etl_code
upload_etl_code >> create_cluster
create_cluster >> add_jobflow_steps
add_jobflow_steps >> trip_processing
add_jobflow_steps >> station_processing
add_jobflow_steps >> weather_processing
add_jobflow_steps >> region_processing
trip_processing >> data_quality_check
station_processing >> data_quality_check
weather_processing >> data_quality_check
region_processing >> data_quality_check
data_quality_check >> end_operator