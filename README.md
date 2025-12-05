📁 Repository Structure — File & Dataset Descriptions
This repository contains all components of the NYC Energy Consumption ETL Pipeline, Data Warehouse, MongoDB storage, and visualization deliverables. Below is a detailed description of every file and its purpose in the project.
 
📘 1. DataTransform.ipynb
This notebook performs the Extract–Transform–Load (ETL) process for the NYC Electric Consumption dataset.
It includes:
•	API extraction from NYC Open Data (Socrata API)
•	Data cleaning (date parsing, numeric conversion, handling nulls, removing duplicates)
•	Schema standardization for downstream use
•	Exporting raw and cleaned datasets
This is the primary data-processing notebook for the project.
 
🧾 2. DataWarehouse.sql
Full SQL script that:
•	Creates the dimension tables (Dim_Date, Dim_Location, Dim_Vendor, Dim_Meter, etc.)
•	Creates the Fact_EnergyConsumption fact table with surrogate keys
•	Defines primary/foreign key relationships
•	Structures the star schema used for Redshift
This file represents the logical and physical modeling of the NYC Energy Data Warehouse.
 
☁️ 3. Upload_Data_MangoDBAtlas.ipynb
Notebook used to load the cleaned dataset into MongoDB Atlas, the required cloud storage option for the assignment.
Includes:
•	Connection setup (URI, credentials, server tests)
•	Transformations to JSON-ready format
•	Batch insertion with error handling & retry logic
This notebook validates that the dataset is successfully stored in the cloud.
 
📦 4. electric_consumption_clean.csv.zip
Compressed version of the clean dataset used for:
•	MongoDB insertion
•	Redshift warehouse loading
•	Visualizations
This cleaned dataset reflects post-ETL output.
 
📦 5. electric_consumption_raw.csv.zip
Compressed raw dataset downloaded directly from NYC Open Data.
Included for versioning and reproducibility of the ETL pipeline.
 
📗 6. nyc_electric_consumption_dictionary.csv
Auto-generated Data Dictionary containing:
•	Column names
•	Inferred data types
•	Example sample values
•	Professional descriptions
This file documents the schema used across MongoDB, Redshift, and analytics.
 
🟥 7. nyc_energy_dw_redshift.sql
SQL script tailored for Amazon Redshift, including:
•	Redshift-compatible DDL for all dimensions and fact tables
•	SORTKEY / DISTKEY optimization for large tables
•	COPY statements preparing Redshift loads from S3 (if used)
This script is used when deploying the Data Warehouse to AWS Redshift.
 
📂 8. scripts/
Folder containing additional helper scripts, such as:
•	Python utilities for API pulling
•	Automated ETL pipeline components
•	Batch insertion helpers
•	Additional transformation modules
This directory supports reproducibility and modular code design.
 
📊 Datasets Used
NYC Electric Consumption (2010–2025) – Socrata Open Data
Used via API:
https://data.cityofnewyork.us/Housing-Development/Electric-Consumption-And-Cost-2010-May-2025-/jr24-e7cr
The dataset includes:
•	Development name & borough
•	Meter & vendor details
•	Consumption (kWh / kW)
•	Billing period dates
•	Charges & cost data
This dataset powers:
•	MongoDB storage
•	The AWS Redshift Data Warehouse
•	All visualizations

