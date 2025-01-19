# นำเข้า libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils.dates import days_ago
from scripts.slack_notify import *
import pandas as pd
import requests
import json

# กำหนด Database connection, Path file และ dataset
SQLITE_CONNECTION = "sqlite_default"
CONVERSION_RATE_URL = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"

local_conversion_rate_path = "/opt/airflow/data/conversion_rate.csv"
local_database_path = "/opt/airflow/data/audible_data_merged.csv"
local_transform_path = "/opt/airflow/data/audible_data_transformed.csv"
local_schema_path = "/opt/airflow/data/r2de2_schema.json"

gcs_bucket_name = "r2de2-data-lake-sbmk"
gcs_conversion_rate_path = "data/conversion_rate.csv"
gcs_database_path = "data/audible_data_merged.csv"
gcs_transform_path = "data/audible_data_transformed.csv"

bq_dataset = "r2de2_workshop_dataset_sbmk"
bq_table_name = "r2de2_workshop"

# Function: Local to Cloud Storage (Data Lake)
def local_to_gcs(local_file_path, gcs_file_path, gcs_bucket_name, **kwargs):
    load_local_to_gcs = LocalFilesystemToGCSOperator(
        task_id='local_to_gcs',
        src=local_file_path,
        dst=gcs_file_path,
        bucket=gcs_bucket_name,
    )
    load_local_to_gcs.execute(context=kwargs)

# กำหนด Default arguments
default_args = {
    'owner': 'r2de2-workshop-sbmk',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': False,
    'retry_delay': False,
    'schedule_interval': '@once',
    'on_success_callback': send_success_notify,
    'on_failure_callback': send_failed_notify,
}

# สร้าง DAG - Directed Acyclic Graph
with DAG(
    'ETL-R2DE2-WORKSHOP',
    default_args=default_args,
    description='Pipeline for ETL retail data from SQLite to BigQuery with Airflow in Docker',
    tags=['r2de2-workshop']
) as dag:

    # Task 1: Extract coversion rate
    def get_conversion_rate(url, local_file_path, gcs_file_path, gcs_bucket_name):
        # อ่าน data จาก REST API
        r = requests.get(url)
        result_conversion_rate = r.json()

        # แปลงเป็น DataFrame
        df = pd.DataFrame(result_conversion_rate)

        # แปลงจาก column 'index' เป็น column 'date' และ reset index ใหม่
        conversion_rate = df.reset_index().rename(columns={"index": "date"})
        conversion_rate.to_csv(local_file_path, index=False)

        # Load raw data of coversion rate to Cloud Storage (Data Lake)
        local_to_gcs(local_file_path, gcs_file_path, gcs_bucket_name)

        print(f"Output to Local: {local_file_path} and GCS: {gcs_file_path}")
        print("== Task 1: Extract coversion rate is complete! ʕ•́ᴥ•̀ʔっ♡ ==")
    
    extract_conversion_rate = PythonOperator(
        task_id="extract_conversion_rate",
        python_callable=get_conversion_rate,
        op_kwargs={"url": CONVERSION_RATE_URL,
                   "local_file_path": local_conversion_rate_path,
                   "gcs_file_path": gcs_conversion_rate_path,
                   "gcs_bucket_name": gcs_bucket_name
                   },
    )
    
    # Task 2: Extract data from SQLite
    def get_data_from_sqlite(sql_conection, local_file_path, gcs_file_path, gcs_bucket_name):
        sqlserver = SqliteHook(sql_conection)
        audible_data = sqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
        audible_transaction = sqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

        # เปลี่ยนชื่อ column เป็น lowercase และเปลี่ยนช่องว่าง (" ") เป็น underscore ("_") และลบจุด (".") ออก
        audible_data.columns = [c.lower().replace(" ", "_").replace(".", "") for c in audible_data.columns]
        audible_transaction.columns = [c.lower().replace(" ", "_") for c in audible_transaction.columns]

        # Merge data จาก 2 DataFrame
        audible_data_merged = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="book_id")
        audible_data_merged.to_csv(local_file_path, index=False)

        # Load raw data of SQLite to Cloud Storage (Data Lake)
        local_to_gcs(local_file_path, gcs_file_path, gcs_bucket_name)

        print(f"Output to Local: {local_file_path} and GCS: {gcs_file_path}")
        print("== Task 2: Extract data from SQLite is complete! ʕ•́ᴥ•̀ʔっ♡ ==")
    
    extract_database = PythonOperator(
        task_id="extract_data_from_sqlite",
        python_callable=get_data_from_sqlite,
        op_kwargs={"sql_conection": SQLITE_CONNECTION,
                   "local_file_path": local_database_path,
                   "gcs_file_path": gcs_database_path,
                   "gcs_bucket_name": gcs_bucket_name
                   },
    )

    # Task 3: Transform data
    def merge_data(conversion_rate_path, database_path, local_file_path, gcs_file_path, gcs_bucket_name):
        # อ่านไฟล์ csv
        conversion_rate = pd.read_csv(conversion_rate_path)
        transaction = pd.read_csv(database_path)

        # ลบ row ที่ column ชื่อ timestamp เท่ากับ '2021-06'
        transaction = transaction.drop(transaction[transaction.timestamp =='2021-06'].index)

        # ก็อปปี้ column 'timestamp' เก็บเอาไว้ใน column ใหม่ชื่อ 'date'
        transaction['date'] = transaction['timestamp']

        # แปลงจาก timestamp เป็น date ในทั้ง 2 DataFrame
        transaction['date'] = pd.to_datetime(transaction['date']).dt.date
        conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

        # Merge data จาก 2 DataFrame
        final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date") # merge 2 DataFrame
        
        # แปลงราคา โดยเอาเครื่องหมาย $ ออก และแปลงให้เป็น float
        final_df["price"] = final_df.apply(lambda x: x["price"].replace("$",""), axis=1)
        final_df["price"] = final_df["price"].astype(float)

        # เพิ่ม column 'THBPrice' และคำนวณจาก column 'price' * 'conversion_rate'
        final_df["THBprice"] = final_df["price"] * final_df["conversion_rate"]

        # ลบ column 'date'
        final_df = final_df.drop("date", axis=1)
        final_df.to_csv(local_file_path, index=False)

       # Load data of transformation to Cloud Storage (Data Lake)
        local_to_gcs(local_file_path, gcs_file_path, gcs_bucket_name)

        print(f"Output to Local: {local_file_path} and GCS: {gcs_file_path}")
        print("== Transform data is complete! ʕ•́ᴥ•̀ʔっ♡ ==")
    
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=merge_data,
        op_kwargs={"conversion_rate_path": local_conversion_rate_path,
                   "database_path": local_database_path,
                   "local_file_path": local_transform_path,
                   "gcs_file_path": gcs_transform_path,
                   "gcs_bucket_name": gcs_bucket_name
                   },
    )
    
    # Task 4: Load transformed data from Cloud Storage (Data Lake) to BigQuery (Data Warehouse)
    with open(local_schema_path) as json_file:
        transformed_schema = json.load(json_file)

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket=gcs_bucket_name,
        source_objects=[gcs_transform_path],
        destination_project_dataset_table=f'{bq_dataset}.{bq_table_name}',
        skip_leading_rows=1,
        schema_fields=transformed_schema,
        autodetect=False,
        write_disposition='WRITE_TRUNCATE',
    )

    # สร้าง Task dependencies
    [extract_conversion_rate, extract_database] >> transform_data >> load_gcs_to_bq