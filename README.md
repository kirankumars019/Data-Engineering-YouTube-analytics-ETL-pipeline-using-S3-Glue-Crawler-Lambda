YouTube Analytics ETL Pipeline on AWS

A fully executed end-to-end Data Engineering project that builds a scalable ETL pipeline to process YouTube trending-video data using AWS serverless services.

Overview

This project implements an automated ETL pipeline for ingesting, cleaning, transforming, and analyzing YouTube trending-video datasets. The pipeline processes structured and semi-structured data from multiple regions, enriches it with category metadata, and stores the final analytics-ready dataset in S3 for querying and dashboarding.
I executed this project using AWS services to simulate a real production-grade data pipeline.

Project Goals
1)	Data Ingestion: Automatically load raw JSON/CSV data into S3.
2)	ETL Pipeline: Clean and transform raw data into analytics-ready format using Lambda and Glue.
3)	Data Lake: Maintain separate raw, cleansed, and analytics layers in S3.
4)	Scalability: Use serverless technologies that scale automatically with data growth.
5)	Cloud-Native: Leverage AWS for processing large datasets reliably.
6)	Reporting: Build dashboards to analyze trends across categories, regions, and metrics.

AWS Services Used
1) Amazon S3 – Central data lake storing raw, cleansed, and analytics datasets.
2) AWS IAM – Secure access control for Lambda, S3, Glue, Crawlers, and Athena.
3) AWS Lambda – Processes raw JSON files and writes clean Parquet data to S3.
4) AWS Glue –
• Crawlers to classify and catalog data
• ETL jobs to join raw & reference datasets and build the final analytics table
5) Amazon Athena – Serverless SQL queries on S3-stored data.
6)Amazon QuickSight – Dashboard and visualization of YouTube analytics.

Dataset
The project uses the YouTube Trending Videos dataset from Kaggle.
It includes:
1)	Daily trending videos across multiple regions
2) Video metadata: title, channel, publish time, tags
3)	Engagement metrics: views, likes, dislikes, comments
4)	Region-specific category_id
5)	A matching JSON file containing category metadata

Data formats: CSV + JSON, structured and semi-structured.

