import os
import logging

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

from google.cloud import storage

from airflow.models.connection import Connection
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from kaggle.api.kaggle_api_extended import KaggleApi

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


dataset_file = "player_stats.csv"
dataset_url = f"https://www.kaggle.com/datasets/rehandl23/fifa-24-player-stats-dataset?select={dataset_file}"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
updated_parquet_file = "part-00000-d5a8e06f-8586-4da0-89fa-3ced93bcd153-c000.snappy.parquet"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'ucl_data')
csv_source = AIRFLOW_HOME+'/kaggle/data'


gcp_conn_id = 'my_connection'
conn = Connection(
    gcp_conn_id,
    'google_cloud_platform',
    extra={
        'extra__google_cloud_platform__keyfile': '/home/bryan/.google/credentials/google_credentials.json',
        'extra__google_cloud_platform__project': PROJECT_ID
    }
)

def download_from_kaggle(downloadpath):
    dataset = 'rehandl23/fifa-24-player-stats-dataset'
    path = downloadpath
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path,unzip=True)

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


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        retries = 1,
        python_callable = download_from_kaggle,
        op_kwargs = {
            "downloadpath": csv_source
        }
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{csv_source}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{csv_source}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    updated_bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="updated_bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "updated_types",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/par/{updated_parquet_file}"],
            },
        },
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task >> updated_bigquery_external_table_task