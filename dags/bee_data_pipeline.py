from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import os
import sys

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
    data_pipeline(FILE_PATH_SENSOR1, FILE_PATH_SENSOR2)


# Create a task using PythonOperator
run_pipeline_task = PythonOperator(
    task_id="run_spark_pipeline",
    python_callable=run_spark_pipeline,
    dag=dag,
)
