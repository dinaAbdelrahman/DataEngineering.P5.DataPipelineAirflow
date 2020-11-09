# Building DataPipeline using Apache Airflow

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect  to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


## Project Overview

This project will introduce the core concepts of Apache Airflow. I will create my custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

![Dag View](https://github.com/dinaAbdelrahman/DataEngineering.P5.DataPipelineAirflow/blob/master/Dag_project.JPG)

## Add Airflow Connections

Here, we'll use Airflow's UI to configure your AWS credentials and connection to Redshift.

To go to the Airflow UI:
You can use the Project Workspace here and click on the blue Access Airflow button in the bottom right.
If you'd prefer to run Airflow locally, open http://localhost:8080 in Google Chrome (other browsers occasionally have issues rendering the Airflow UI).
Click on the Admin tab and select Connections.

Under Connections, select Create.

On the create connection page, enter the following values:

Conn Id: Enter aws_credentials.
Conn Type: Enter Amazon Web Services.
Login: Enter your Access key ID from the IAM User credentials you downloaded earlier.
Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.
Once you've entered these values, select Save and Add Another.


On the next create connection page, enter the following values:

Conn Id: Enter redshift.
Conn Type: Enter Postgres.
Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
Schema: Enter dev. This is the Redshift database you want to connect to.
Login: Enter awsuser.
Password: Enter the password you created when launching your Redshift cluster.
Port: Enter 5439.
Once you've entered these values, select Save.



Now all configured to run Airflow with Redshift.

## Datasets
For this project, I'll be working with two datasets. Here are the s3 links for each:

Log data: s3://udacity-dend/log_data </br>
Song data: s3://udacity-dend/song_data
