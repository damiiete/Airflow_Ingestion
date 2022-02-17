# Airflow_Ingestion

## Introduction
This project uses [Apache Airflow](https://airflow.apache.org/) to create workflow ocherstartion of a data pipeline that extracts,
changes the format and uploads the data to a data lake (GCS).

## Process
- The airflow is installed on using the official arflow docker image.
- DAGs are created in a python file.
- The DAGs are triggered from the airflow webserver UI.

## Download and Transformation of Data
The [NYC taxi trip data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) was used for this project.
- Downloaded the data using `curl`.
- Data was transformed using the `pandas` library from csv to parquet (compressed format easier for querying).

## Uploading Data
- The data was uplaoded using `google.cloud` python library
