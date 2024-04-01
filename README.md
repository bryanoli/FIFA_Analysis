# FIFA Analysis
The purpose of this project is to create a pipeline that ingest the kaggle dataset of players statistics of FIFA 24 and produce a visualization of the best strikers from different countries based on attacking attributes such as volleys, finishing, shot_power, etc ...

# Tools
The tools that were used in this project were:
- Google Cloud: Where the whole project operated
- Airflow: To orchestrate the ETL operations
- Terraform: To programmatically instantiate different cloud components
- Google Cloud Storage (GCS): To store different types of data
- BigQuery: To store, transform and analyse large amounts of data
- Spark: To transform data
- Looker Studio: To visualise data kept in BigQuery

# Steps to Reproduce
Install the below tools:
-Google Cloud SDK
-Terraform
-Docker

Cloud Setup
In GCP, you have to create a service principal with the following permissions:
-Storage Admin
-BigQuery Admin
-Storage Object Admin

-Download the service authentication file and save it in your home directory
`$HOME/.google/credentials/google_credentials.json`

-Ensure that the following API are enable in your Google Cloud:
    - Compute Engine API
    - BigQuery API
    - BigQuery Storage API
    - IAM API
    - IAM Service Credentials API
#Terraform
- Perform the following commands to set up Terraform
`cd terraform
 terraform init
 terraform plan
 terraform apply`

# AIRFLOW 
-Set up Airflow with these docker-compose commands.
`cd airflow
docker-compose build
docker-compose up airflow-init
docker-compose up`
- Then type `localhost:8080` in your browser and enable the `data_ingestion` dag.
- These will perform the download task, send the parquet file into the datalake which is the google cloud storage, and create the external tables.

# Data Transformation
- You would have to run the jupyter notebook python file of `spark_transformation`
- I could not configure my Airflow to run the spark job.

# Dashboard
[FIFA 24 REPORT](~/UCL_Analysis/FIFA_24_Report.pdf)

