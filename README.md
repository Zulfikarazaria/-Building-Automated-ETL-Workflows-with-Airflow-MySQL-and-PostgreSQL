# Building Automated ETL Workflows with Airflow, MySQL, and PostgreSQL

## Project Overview
This project demonstrates how to implement an automated ETL (Extract, Transform, Load) workflow using Apache Airflow, MySQL, and PostgreSQL. It involves extracting data from a MySQL database, processing it in a staging area, and storing it in PostgreSQL. After the ETL process, the project performs a simple aggregation analysis and emails the results daily.

## Key Features
- **ETL Process**: Extracts data from MySQL at 7 AM daily, transforms it, and stores it in PostgreSQL using Parquet format for efficient storage and processing.
- **Data Aggregation**: Summarizes sales data by transaction date and product name, calculating total sales and total quantity.
- **Automated Reports**: Sends daily sales reports via email at 9 AM in Excel format. If any error occurs, failure notifications are triggered automatically.
- **Three DAGs**: 
  1. **etl_dag** - Handles data extraction and loading.
  2. **aggregation_dag** - Processes the aggregated sales data.
  3. **send_analysis_notification_dag** - Sends the report via email.

## Problem Statement
The project automates daily data transfer from MySQL to PostgreSQL and ensures that updated sales reports are sent automatically without manual intervention. This workflow is designed to handle large datasets efficiently while ensuring data accuracy and availability.

## Data Flow
1. **Data Extraction**: MySQL hooks retrieve data based on the previous day's transactions.
2. **Staging**: Data is temporarily stored in Parquet format for efficient processing.
3. **Data Aggregation**: Aggregates sales data (total sales, total quantity) and stores it in the `sales_aggregated` table.
4. **Report Delivery**: Results are sent via email at 9 AM in Excel format.

## Technologies Used
- **Airflow**: For workflow scheduling and orchestration.
- **MySQL**: As the source database for raw data.
- **PostgreSQL**: As the destination database for processed data.
- **Parquet**: For temporary storage during the staging process.
- **SQLAlchemy**: For efficient batch inserts.
- **Excel**: For the final sales report.
