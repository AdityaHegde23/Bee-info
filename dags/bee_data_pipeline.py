from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime
from dotenv import load_dotenv
from airflow.models import TaskInstance
from airflow.utils import db
from prometheus_client import start_http_server, Counter
import time
import os
import sys
import logging
import threading  # need to run prometheus in different thread.

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from bee_sensor_transform import data_pipeline


# Load environment variables from .env
load_dotenv()

# Access the environment variables
GCS_BUCKET_PATH = os.getenv("GCS_BUCKET_PATH")
FILE_PATH_SENSOR1 = os.getenv("FILE_PATH_SENSOR1")
FILE_PATH_SENSOR2 = os.getenv("FILE_PATH_SENSOR2")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

print(f"Google Cloud Storage Bucket Path: {GCS_BUCKET_PATH}")
print(f"Local Path: {FILE_PATH_SENSOR1}\n {FILE_PATH_SENSOR2}")
print(f"Airflow Home: {AIRFLOW_HOME}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 1),
    "retries": 1,
}

# Define the DAG
dag = DAG(
    "bee_data_pipeline",
    default_args=default_args,
    description="Pipeline for processing bee sensor data",
    schedule_interval="@daily",  # Run daily
    catchup=False,
)


# Define the task to run the Spark pipeline
def run_spark_pipeline():
    logging.info("Starting transformation")
    data_pipeline(FILE_PATH_SENSOR1, FILE_PATH_SENSOR2)
    logging.info("....Done....")


def check_pipeline_status(**kwargs):
    ti = kwargs["ti"]
    run_bee_sensor_data_transform_pipeline_task_status = ti.xcom_pull(
        task_ids="data_pipeline"
    )
    return run_bee_sensor_data_transform_pipeline_task_status


# Create a task using PythonOperator
run_bee_sensor_data_transform_pipeline_task = PythonOperator(
    task_id="run_spark_pipeline",
    python_callable=run_spark_pipeline,
    dag=dag,
)

# Task to check the status and send email
status_check_task = PythonOperator(
    task_id="check_status",
    python_callable=check_pipeline_status,
    provide_context=True,
    dag=dag,
)

# e-mail alerts
email_alert = EmailOperator(
    task_id="send_email",
    to="ahegd005@ucr.edu",
    subject="Airflow Alert: Task {{run_bee_sensor_data_transform_pipeline_task_status}}",
    html_content="Task {{ task_instance.task_id }} {{run_bee_sensor_data_transform_pipeline_task_status}}.",
    dag=dag,
)


# Set task dependencies
run_bee_sensor_data_transform_pipeline_task >> status_check_task >> email_alert
