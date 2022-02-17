from datetime import datetime
import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def download_format_upload(
    dag,
    URL_TEMPLATE,
    CSV_FILE_TEMPLATE,
    PARQUET_FILE_TEMPLATE,
    PARQUET_FILE,
    GCS_FOLDER
    ):
    with dag:

        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {URL_TEMPLATE} > {CSV_FILE_TEMPLATE}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": CSV_FILE_TEMPLATE,
            },
        )
        
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"dtc_de_week2/{GCS_FOLDER}/{PARQUET_FILE}",
                "local_file": PARQUET_FILE_TEMPLATE,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {CSV_FILE_TEMPLATE} {PARQUET_FILE_TEMPLATE}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task


default_args = {
    "owner": "Damiiete",
    "depends_on_past": False,
    "retries": 1,
}

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'

#YELLOW TRIPS DATA
# https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv

YELLOW_URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_CSV_FILE_TEMPLATE = AIRFLOW_HOME_PATH + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME_PATH + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_PARQUET_FILE= 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_GCS_FOLDER = 'yellow_taxi_trips'

yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data_v2",
    schedule_interval="0 5 2 * *",
    default_args=default_args,
    start_date= datetime(2019, 1, 1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-personal-project'],
)

download_format_upload(
    dag=yellow_taxi_data_dag,
    URL_TEMPLATE=YELLOW_URL_TEMPLATE,
    CSV_FILE_TEMPLATE=YELLOW_CSV_FILE_TEMPLATE,
    PARQUET_FILE_TEMPLATE=YELLOW_PARQUET_FILE_TEMPLATE,
    PARQUET_FILE=YELLOW_PARQUET_FILE,
    GCS_FOLDER=YELLOW_GCS_FOLDER
)

#GREEN TRIPS DATA
# https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2021-01.csv

GREEN_URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
GREEN_CSV_FILE_TEMPLATE = AIRFLOW_HOME_PATH + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
GREEN_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME_PATH + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_PARQUET_FILE= 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_GCS_FOLDER = 'green_taxi_trips'

green_taxi_data_dag = DAG(
    dag_id="green_taxi_data_v1",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-personal-project'],
)

download_format_upload(
    dag=green_taxi_data_dag,
    URL_TEMPLATE=GREEN_URL_TEMPLATE,
    CSV_FILE_TEMPLATE=GREEN_CSV_FILE_TEMPLATE,
    PARQUET_FILE_TEMPLATE=GREEN_PARQUET_FILE_TEMPLATE,
    PARQUET_FILE=GREEN_PARQUET_FILE,
    GCS_FOLDER=GREEN_GCS_FOLDER
)

#FHV TRIP DATA
# https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2021-01.csv

FHV_URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_CSV_FILE_TEMPLATE = AIRFLOW_HOME_PATH + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME_PATH + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_PARQUET_FILE= 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_GCS_FOLDER = "fhv_trips"

fhv_taxi_data_dag = DAG(
    dag_id="hfv_taxi_data_v1",
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-personal-project'],
)

download_format_upload(
    dag=fhv_taxi_data_dag,
    URL_TEMPLATE=FHV_URL_TEMPLATE,
    CSV_FILE_TEMPLATE=FHV_CSV_FILE_TEMPLATE,
    PARQUET_FILE_TEMPLATE=FHV_PARQUET_FILE_TEMPLATE,
    PARQUET_FILE=FHV_PARQUET_FILE,
    GCS_FOLDER=FHV_GCS_FOLDER
)

#ZONES DATA
# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

ZONES_URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
ZONES_CSV_FILE_TEMPLATE = AIRFLOW_HOME_PATH + '/taxi_zone_lookup.csv'
ZONES_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME_PATH + '/taxi_zone_lookup.parquet'
ZONES_PARQUET_FILE="taxi_zone_lookup.parquet"
ZONES_GCS_FOLDER = "taxi_zone"

zones_data_dag = DAG(
    dag_id="zones_data_v1",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-personal-project'],
)

download_format_upload(
    dag=zones_data_dag,
    URL_TEMPLATE=ZONES_URL_TEMPLATE,
    CSV_FILE_TEMPLATE=ZONES_CSV_FILE_TEMPLATE,
    PARQUET_FILE_TEMPLATE=ZONES_PARQUET_FILE_TEMPLATE,
    PARQUET_FILE=ZONES_PARQUET_FILE,
    GCS_FOLDER=ZONES_GCS_FOLDER
)