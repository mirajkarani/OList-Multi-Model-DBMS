# Retail Management System

<h2>Overview</h2>

• In this project, our objective is to build a data pipeline for multi-model databases for JSON data format as well as graph database using Microsoft Azure Cosmos DB SQL and Gremlin APIs. <br/>
• We have the OList Brazilian e-commerce dataset from Kaggle for building and simulating our data pipeline. <br/>
• The first step in our data pipeline is to extract data from the CSV files, perform data cleansing and load the data to SQL server using Talend Open Studio. <br/>
• In the next step, data orchestration is performed using Apache Airflow to load data into Blob Containers using Python scripts. Once the json files are created in Blob container, the Data Factory scripts load the data to Cosmos DB. <br/>
• The data from Cosmos DB is then used for performing visualization and reporting using Power BI and Gremlin IDE. <br/>

<h2> Change Data Capture & Scheduling</h2>

• We have scheduled our Talend workflow jobs using Windows Task Scheduler to run daily at 14:00 PM EST. This will kick off all the ETL and staging workflow and load data into staging SQL Server Database locally. <br/>  
• Next, in Apache Airflow we have setup DAG to schedule data pipeline job run, which will load the JSON file from SQL Server using SQL API for Cosmos DB into the Azure Blob Containers at daily 15:00 PM EST. <br/>
• As soon as the JSON file is created in Azure Blob Container, it will trigger an Azure Data Factory pipeline workflow automatically and load the JSON data into Azure Cosmos DB with ‘upsert’ configuration which will handle Delta load and insert/update the data accordingly. <br/>
• Also, Gremlin Data Load script is scheduled using Windows Scheduler to run daily 15:00 PM EST, which will load the report data into Azure Cosmos DB for Gremlin API graph db. <br/>

<h2>Architecture Diagram</h2>

![alt text](https://i.imgur.com/tqJTzjW.png)

<h2>Technologies Used</h2>

• Microsoft SQL Server <br/>
• Talend Open Studio for DI <br/>
• Microsoft Azure (Blob Container, Data Factory, CosmosDB for SQL & Gremlin API) <br/>
• Apache Airflow <br/>
• Power BI <br/>
• Gremlin Graph Database IDE <br/>
