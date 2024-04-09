# FIFA Analysis

The purpose of this project is to create a pipeline that ingests the [Kaggle Dataset](https://www.kaggle.com/datasets/rehandl23/fifa-24-player-stats-dataset) of player statistics of FIFA 24 and produces a visualization of the best strikers from different countries based on attacking attributes such as volleys, finishing, shot power, etc.

## Tools

The tools used in this project include:

- Google Cloud: Where the entire project operates
- Airflow: To orchestrate the ETL operations
- Terraform: To programmatically instantiate different cloud components
- Google Cloud Storage (GCS): To store various types of data
- BigQuery: To store, transform, and analyze large amounts of data
- Spark: To transform data
- Looker Studio: To visualize data stored in BigQuery

## Steps to Reproduce

### Install the Required Tools:

- Google Cloud SDK
- Terraform
- Docker

### [Cloud Setup](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=13)

In Google Cloud Platform (GCP), create a service account with the following permissions:

- Storage Admin
- BigQuery Admin
- Storage Object Admin

Download the service account authentication file and save it in your home directory:

`$HOME/.google/credentials/google_credentials.json`

Ensure that the following APIs are enabled in your Google Cloud project:

- Compute Engine API
- BigQuery API
- BigQuery Storage API
- IAM API
- IAM Service Credentials API

### [Terraform Setup](https://www.youtube.com/watch?v=Hajwnmj0xfQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=13)

Navigate to the Terraform directory and execute the following commands:

`cd terraform`  
`terraform init`  
`terraform plan`  
`terraform apply`  

### Airflow Setup

Set up Airflow with Docker Compose:

`cd airflow`
`docker-compose build`
`docker-compose up airflow-init`
`docker-compose up`
- Then type `localhost:8080` in your browser and enable the `data_ingestion` dag.
- These will perform the download task, send the parquet file into the datalake which is the google cloud storage, and create the external tables.

# Data Transformation
- You would have to run the jupyter notebook python file of `spark_transformation`
- I could not configure my Airflow to run the spark job.

# Dashboard
[FIFA 24 REPORT](FIFA_24_Report.pdf)

