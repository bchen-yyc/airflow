# Data Pipelines with Airflow

## Background
This project is built on top of the [AWS Data Warehouse project], and meant to be run on a on-premise Airflow platform.

The music streaming company, Sparkify, wants to automate and monitor their data warehouse ETL pipelines using Apache Airflow. Our goal is to build high-grade data pipelines that are dynamic and built from reusable tasks, can be monitored and allow easy backfills. We also want to improve data quality by running tests against the dataset after the ETL steps are complete in order to catch any discrepancies.

The source data is in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source data are json logs containing user activity in the application and JSON metadata about the songs the users listen to. 

## Airflow Tasks

We will create custom operators to perform tasks such as staging the data, filling the data warehouse and running checks. The tasks will need to be linked together to achieve a coherent and sensible data flow within the pipeline. 

## Datasets

The log data is located at `s3://datalbc/input/log-data/` and the song data is located in `s3://datalbc/input/song-data/`.

## Project Template

- There are three major components of the project:
1. Dag template with all imports and task templates.
2. Operators folder with operator templates.
3. Helper class with SQL transformations.
- Add `default parameters` to the Dag template as follows:
    * Dag does not have dependencies on past runs
    * On failure, tasks are retried 3 times
    * Retries happen every 5 minutes
    * Catchup is turned off
    * Do not email on retry
- The task dependencies should generate the following graph view:
![Fig 1: Dag with correct task dependencies](plugins\helpers\dag.png)
- There are four operators:
1. stage_redshift operator 
    * Loads JSON files from S3 to Amazon Redshift with `SQL COPY` statement 
    * Contain a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills
2. load_fact Operators
    * load fact table with query specified in `sql_queries.py` file
3. load_dimension Operators
    * load dimenstion tables with selected columns from stage tables
4. Data Quality Operator
    * Run checks on the data
    * Receives one or more SQL based test cases along with the expected results and executes the tests
    * Test result and expected results are checked and if there is no match, operator should raise an exception and the task should retry and fail eventually

## Files in repository and how to run them

- `create_tables.sql` contains the queries when you provision the Redshift clusters in order to create the tables, it is not a step of the DAG.
- `airflow_pipelne.py` is the main code to organize the DAG.
- `plugins\helpers\sql_queries` contains the queries to load all the tables.
- `plugins\operators\` contains all the operators which are alled by `airflow_pipelne.py`.

[AWS Data Warehouse project]:<https://github.com/bchen-yyc/aws_data_warehouse>