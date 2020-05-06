# Data Pipelining with Apache Airflow

Data Pipelines with Airflow project in the context of Udacity's Data
Engineering Nanodegree.

In this project, we build data pipelines for the Data Warehouse of a mock
startup called ***Sparkify*** for its music streaming app. The idea is to
create high grade data pipelines that are dynamic and built from reusable
tasks, can be monitored, and allow easy backfills.

We will create the necessary pipeline *operators* and *dags* for the data
pipelines to work as expected, using Apache Airflow as an orchestarator.


## ETL design

The source data resides in S3 and needs to be processed in Sparkify's data
warehouse in Amazon Redshift. The source datasets consist of JSON logs that
tell about user activity in the application and JSON metadata about the songs
the users listen to.

The *create_tables.sql* SQL script contains all the SQL transformations needed
in the ETL process. The orchestration of task will be performed through our
pipelines.

## Project structure

The project is divided in three parts:

- **Dags** folder containing the pipeline DAG creation to use in
    *Apache Airflow*.

- **Plugins** folder which contains the pipeline Operators to use in the
    previous pipelines. This folder also contains a helper SQL queries class.

- **Create SQL script**, which contains all the SQL instructions to build the
    data warehouse tables.

- **README** file with the project's documentation.

## Project run instructions

To run the project, follow the steps:

1. Make sure the Apache Airflow Scheduler and Webserver are properly running.

2. Make sure Airflow has the dags and plugin folders assigned with our Python
    scripts.

3. Make sure Airflow has the necessary variables and connections for correct
    execution of the pipelines. This includes the **aws_credentials** and
    **redshift** connections.

4. Inside the Airflow Webserver we can execute the **udac_example_dag** DAG to
    run our pipeline.
