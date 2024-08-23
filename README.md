# AWS-Data-Lakehouse-for-STEDI-Human-Balance-Analytics

Welcome to the repository for the AWS Data Lakehouse for STEDI's Human Balance Analytics project. This project demonstrates how to build an AWS-based data lakehouse to handle, sanitize, and analyze data collected from STEDI Step Trainers and their companion mobile application. The ultimate goal is to train a machine learning model to accurately detect steps in real-time, with a strong emphasis on privacy and data integrity.

## Project Overview
STEDI has developed a cutting-edge STEDI Step Trainer, designed to help users perform balance exercises and collect valuable sensor data. This device, combined with a mobile application, generates data in multiple formats:

### Step Trainer: 
Records distance measurements of detected motion.

### Mobile Application: 
Uses an accelerometer to capture motion in the X, Y, and Z directions.
With millions of early adopters ready to use these devices, STEDI aims to leverage the collected data to improve step detection algorithms. However, due to privacy concerns, only data from customers who have explicitly agreed to share their data for research purposes will be used in the machine learning model training.

## Project Requirements

### Data Simulation:
Create and upload data into S3 directories:
customer_landing
step_trainer_landing
accelerometer_landing

### Glue Tables Creation:
Develop Glue tables for customer_landing,accelerometer_landing and step_trainer_landing using the provided SQL scripts.
Query these tables using Athena and capture screenshots:

### Data Sanitization and Transformation:
Customer Data: Create a Glue job to sanitize and store customer records who agreed to share their data in customer_trusted.
Accelerometer Data: Create a Glue job to sanitize and store accelerometer readings from customers who agreed to share their data in accelerometer_trusted.
Verify the success of these Glue jobs and capture the screenshots from athena.

### Handling Data Quality Issues:
Customers Curated Data: Create a Glue job to generate the customers_curated table (data cleaning to remove redundant records), integrating sanitized data from trusted zones.
Step Trainer Data: Develop Glue Studio jobs to: Populate a trusted zone Glue table called step_trainer_trusted using Step Trainer 
data from customers who have accelerometer data. Finally, Create an aggregated table machine_learning_curated combining step trainer and accelerometer data for the same timestamp.

## Solution

### Data Upload:
Uploaded the source datasets (customer, accelerometer, and step_trainer) into the designated S3 landing folders.

### Infrastructure Setup:
Script: Created a Python script for setting up IAM roles and policies using Infrastructure as Code (IaC). Configured IAM roles for AWS Glue, EC2, CloudWatch, and VPC for security and logging.

### AWS Glue Configuration:
Set up Glue tables and schemas for customer_landing, accelerometer_landing, and step_trainer_landing using the AWS Glue Console.
ETL Jobs: Created ETL jobs to transform data from landing zones to trusted zones and validated results using Athena. Stored processed JSON data back into S3 in the corresponding folders.

### Curated Data Preparation:
Created additional Glue tables: customer_curated and machine_learning_curated from trusted zones.
Verified data accuracy and completeness using Athena queries and screenshots.

## Screenshots
### Below are the screenshots capturing the data results for various Glue tables:

![alt text](Screenshots/customer_landing.png)

![alt text](Screenshots/customer_landing.png)

![alt text](Screenshots/customer_landing.png)

![alt text](Screenshots/customer_landing.png)

![alt text](Screenshots/customer_landing.png)

![alt text](Screenshots/customer_landing.png)
