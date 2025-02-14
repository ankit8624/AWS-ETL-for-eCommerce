# AWS-ETL-for-eCommerce


This project implements an ETL pipeline using **Apache Airflow** to extract data from CSV files, transform it, and store it in **Amazon S3**. The processed data is then visualized using **Amazon QuickSight**.

## Architecture  
The pipeline follows this workflow:

1. **Extract** – Read raw eCommerce data from CSV files.  
2. **Transform** – Process and clean the data using Airflow.  
3. **Load** – Store the transformed data in Amazon S3.  
4. **Visualize** – Use Amazon QuickSight to create interactive dashboards.  


## Features  
- Automated ETL pipeline with **Apache Airflow**  
- Scalable storage using **Amazon S3**  
- Data visualization with **Amazon QuickSight**  

