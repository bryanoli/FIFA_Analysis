import os
import logging

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
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
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'ucl_data')
csv_source = AIRFLOW_HOME+'/kaggle'

def download_from_kaggle(downloadpath):
    dataset = 'rehandl23/fifa-24-player-stats-dataset'
    path = downloadpath
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path,unzip=True)
    # api.dataset_download_file(dataset, 'artists.csv', path,)
    # api.dataset_download_file(dataset, 'tracks.csv', path,unzip=True)

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """


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
            "src_file": f"./dags/data/player_stats.csv",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/player_stats.parquet",
            "local_file": f"./dags/data/player_stats.parquet",
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
                "source_Format": "PARQUET",
                "source_Uris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task



# URL_PREFIX = 'https://www.kaggle.com/datasets/rehandl23/fifa-24-player-stats-dataset?select='

# UCL_URL_TEMPLATE = URL_PREFIX + '{{dataset_file}}'
# UCL_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/{{dataset_file}}_{{execution_date.strftime(\'%Y-%m\')}}.csv'
# UCL_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/{{dataset_file}}_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
# UCL_GCS_PATH_TEMPLATE = f"raw/{{dataset_file}}/{{execution_date.strftime('%Y')}}/{{dataset_file}}_{{execution_date.strftime('%Y-%m')}}.parquet"


    # create_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="create_external_table_task",
    #     dataset_id=BIGQUERY_DATASET,
    #     table_id="player_stats_external",
    #     bucket=GCP_GCS_BUCKET,
    #     source_objects=[UCL_GCS_PATH_TEMPLATE.format(dataset_file=dataset_file)],
    #     source_format="PARQUET",
    #     schema_fields=[
    #         {"name": "player", "type": "STRING"},
    #         {"name": "country", "type": "STRING"},
    #         {"name": "height", "type": "INTEGER"},
    #         {"name": "weight", "type": "INTEGER"},
    #         {"name": "age", "type": "INTEGER"},
    #         {"name": "club", "type": "STRING"},
    #         {"name": "ball_control", "type": "INTEGER"},
    #         {"name": "dribbling", "type": "INTEGER"},
    #         {"name": "marking", "type": "STRING"},
    #         {"name": "slide_tackle", "type": "INTEGER"},
    #         {"name": "standing_tackle", "type": "INTEGER"},
    #         {"name": "aggression", "type": "INTEGER"},
    #         {"name": "reactions", "type": "INTEGER"},
    #         {"name": "att_position", "type": "INTEGER"},
    #         {"name": "interceptions", "type": "INTEGER"},
    #         {"name": "vision", "type": "INTEGER"},
    #         {"name": "composure", "type": "INTEGER"},
    #         {"name": "crossing", "type": "INTEGER"},
    #         {"name": "short_pass", "type": "INTEGER"},
    #         {"name": "long_pass", "type": "INTEGER"},
    #         {"name": "acceleration", "type": "INTEGER"},
    #         {"name": "stamina", "type": "INTEGER"},
    #         {"name": "strength", "type": "INTEGER"},
    #         {"name": "balance", "type": "INTEGER"},
    #         {"name": "sprint_speed", "type": "INTEGER"},
    #         {"name": "agility", "type": "INTEGER"},
    #         {"name": "jumping", "type": "INTEGER"},
    #         {"name": "heading", "type": "INTEGER"},
    #         {"name": "shot_power", "type": "INTEGER"},
    #         {"name": "finishing", "type": "INTEGER"},
    #         {"name": "long_shots", "type": "INTEGER"},
    #         {"name": "curve", "type": "INTEGER"},
    #         {"name": "fk_acc", "type": "INTEGER"},
    #         {"name": "penalties", "type": "INTEGER"},
    #         {"name": "volleys", "type": "INTEGER"},
    #         {"name": "gk_positioning", "type": "INTEGER"},
    #         {"name": "gk_diving", "type": "INTEGER"},
    #         {"name": "gk_handling", "type": "INTEGER"},
    #         {"name": "gk_kicking", "type": "INTEGER"},
    #         {"name": "gk_reflexes", "type": "INTEGER"},
    #         {"name": "value", "type": "FLOAT"},
    #     ],
    #     google_cloud_storage_conn_id="google_cloud_default",
    # )