export GOOGLE_APPLICATION_CREDENTIALS=/.google/credentials/google_credentials.json
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"

pip install apache-airflow-providers-google apache-airflow-providers-apache-spark kaggle