#!/usr/bin/env bash
export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}
export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=${AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT}
export KAGGLE_USERNAME=bryanolivares
export KAGGLE_KEY=040bce2413a32385d70f7c8f60f4fff8

pip install kaggle

airflow db upgrade

airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow
# "$_AIRFLOW_WWW_USER_USERNAME" -p "$_AIRFLOW_WWW_USER_PASSWORD"

airflow webserver