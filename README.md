# Airflow_Ingestion

## Introduction
This project uses [Apache Airflow](https://airflow.apache.org/) to create workflow ocherstartion of a data pipeline that extracts,
changes the format and uploads the data to a data lake (GCS).

## Process
- The airflow is installed on using the official arflow docker image.
- DAGs are created in a python file.
- The DAGs are triggered from the airflow webserver UI.

