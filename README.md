# Modern-Data-Warehouse
Develop a modern data warehouse using SQL Server to consolidate sakes data, enabling analytical reporting and informed decision-making

# Data Warehouse and Analytics Project
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

---
## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
<img width="1544" height="789" alt="Image" src="https://github.com/user-attachments/assets/e58769fd-93bb-41a3-b2f1-6dc19e3c7b05" />

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:
- SQL Development
- Data Architect
- Data Engineering  
- ETL Pipeline Developer  
- Data Modeling  
- Data Analytics  

---

## 🛠️ Important Links & Tools:

Everything is for Free!
- **[Datasets](datasets/):** Access to the project dataset (csv files).
- **[SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads):** Lightweight server for hosting your SQL database.
- **[SQL Server Management Studio (SSMS)](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16):** GUI for managing and interacting with databases.
---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

<img width="3288" height="1404" alt="Image" src="https://github.com/user-attachments/assets/6d86812f-2283-4176-be06-0c4ec2821639" />

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  

For more details, refer to [docs/requirements.md](docs/requirements.md).

 
  ## 📂 Repository Structure
  
├── .gitignore \
├── LICENSE \
├── README.md
├── Scripts
    ├── bronze
    │   ├── ddl_bronze.sql
    │   └── load_bronze_source.sql
    ├── data_catalog.md
    ├── gold
    │   ├── quality_checks.sql
    │   └── views.sql
    ├── init_database.sql
    └── silver
    │   ├── ddl_silver.sql
    │   ├── quality_checks.sql
    │   └── transformations.sql
└── datasets
    ├── source_crm
        ├── cust_info.csv
        ├── prd_info.csv
        └── sales_details.csv
    └── source_erp
        ├── CUST_AZ12.csv
        ├── LOC_A101.csv
        └── PX_CAT_G1V2.csv
