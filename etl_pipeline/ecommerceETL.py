from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pathlib
import pandas as pd
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 1, #How many times to retry if task gets failed.(Default 0)
    'retry_delay': timedelta(minutes=5),
}

#Reference :https://airflow.apache.org/docs/apache-airflow/2.3.3/_modules/airflow/example_dags/tutorial_etl_dag.html
def extract_data(**kwargs):
    #Path 
    input_file='/home/talentum/shared/Project/data.csv'
    
    try:
        df=pd.read_csv(input_file,encoding='ISO-8859-1')
        print(f"Data extracted from {input_file}")
        ti=kwargs['ti']
        ti.xcom_push(key='ecommerce_data',value=df)
    except Exception as e:
        print(f"Error reading file {input_file}: {e}")
        ti=kwargs['ti']
        ti.xcom_push(key='ecommerce_data',value=None)
# End exctract_function
def transform(**kwargs):
    ti=kwargs['ti']
    df=ti.xcom_pull(task_ids='extract_data',key='ecommerce_data')
    if df is None:
        print("No data to transform")
        return None
    #Data cleaning 
    # Drop the customerID
    df.dropna(subset=['CustomerID'], inplace=True)
    #Fill the description values with no desc
    df['Description'] = df['Description'].fillna('No description available')
    ## Duplicated Values 
    df.drop_duplicates(inplace=True)
    #Drop cancelled orders 
    df = df[~df['InvoiceNo'].str.startswith('C')]
    #Feature Engineering
    
    #handling Anaolies
    anomaly_stock_codes = df[df['StockCode'].str.contains('^[a-zA-Z]',regex=True)]['StockCode']
    for stock_code in anomaly_stock_codes.unique():
        desc = df[df['StockCode'] == stock_code]
    df = df[~df['StockCode'].str.contains('^[a-zA-Z]',regex=True)]

    #Adding new Column
    df['Total price']=df['Quantity']*df['UnitPrice']
    df['StockCode'] = df['StockCode'].astype(str)

    #handle date column
    
    df['InvoiceDate']=df.InvoiceDate.apply(lambda x:x.split(" ")[0])
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate']) 
    df['InvoiceNo'] = pd.to_numeric(df['InvoiceNo'], errors='coerce')
   

    ti.xcom_push(key='transformed_data',value=df)

def load(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform', key='transformed_data')

    if df is not None:
        output_file = '/home/talentum/shared/transformed_data.csv'
        bucket_name = 'ecommerceetl'  # Replace with your S3 bucket
        s3_key = 'transformed_data.csv'  # S3 path

        try:
            # Save locally before uploading
            df.to_csv(output_file, index=False)
            print(f"Transformed data saved to {output_file}")

            # Upload to S3
            s3_hook = S3Hook(aws_conn_id='aws_default')
            s3_hook.load_file(filename=output_file, bucket_name=bucket_name, key=s3_key, replace=True)
            print(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")

        except Exception as e:
            print(f"Error saving or uploading transformed data: {e}")
    else:
        print("No data to load.")

# def load(**kwargs):
#     ti=kwargs['ti']
#     df=ti.xcom_pull(task_ids='transform',key='transformed_data')
#     if df is not None:
#         output_file='/home/talentum/shared/transformed_data.csv'
#         try:
#             df.to_csv(output_file, index=False)
#             print(f"Transformed data saved to {output_file}")
#         except Exception as e:
#             print(f"Error saving transformed data: {e}")
#     else:
#         print("No data to load.")
dag = DAG(
    dag_id="ecommerceETL",
    default_args=default_args,
    description="ecommerce etl",
    start_date=datetime(2025, 2, 8),
    schedule_interval='@daily',  # Runs once per day
    catchup=False,
) 
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task >> load_task  # This defines task order
