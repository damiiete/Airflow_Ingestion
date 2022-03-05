from datetime import datetime
import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
DATASET = "tripdata"

default_args = {
    "owner": "Damiiete",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}  


with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-personal-project'],
) as dag:

    for colour, ds_col in COLOUR_RANGE.items():
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                task_id=f"bq_{colour}_{DATASET}_external_table_task",
                table_resource={
                    "tableReference": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": f"{colour}_{DATASET}_external_table",
                    },
                    "externalDataConfiguration": {
                        "sourceFormat": "PARQUET",
                        "sourceUris": [f"gs://{BUCKET}/dtc_de_week2/{colour}_taxi_trips/*"],
                    },
                },
            )

        CREATE_BQ_TBL_QUERY = (
                f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
                PARTITION BY DATE({ds_col}) \
                AS \
                SELECT * FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
            )

        # Create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                    }
                }
            )

        bigquery_external_table_task >> bq_create_partitioned_table_job