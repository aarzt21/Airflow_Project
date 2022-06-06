# Project: Data Pipelines with Airflow

## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their 
data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The task is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. 
They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests 
against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. 
The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata 
about the songs the users listen to.

## Task
The the task is to create custom airflow operators that perform
- data staging
- inserting the data into the data warehouse
- run sanity checks on the data

## Data 
The data is located in Udacity's S3 buckets:
```
s3://udacity-dend/log_data
```
and 

```
s3://udacity-dend/song_data
```


## DAG Overiew
![alt text](schema.png)

The operators to implement are: 
- Stage operator: Loads JSON and CSV files from S3 buckets to Redshift

- Fact and Dimension Operators: performs data transformations

- Data Quality Operator: performs sanity checks of the data