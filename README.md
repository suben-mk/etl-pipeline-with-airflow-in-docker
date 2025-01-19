# ETL Data Pipeline with Airflow in Docker
จาก Workshop คอร์สเรียนออนไลน์ Road to Data Engineer 2023 โปรเจคนี้ผู้เขียนต้องการเปลี่ยนการจัดการ Apache Airflow ผ่าน Google Cloud Composer เป็นการรัน Apache Airflow บน Docker แทน และแจ้งเตือนเข้า Slack กรณีรัน Task สำเร็จและไม่เสร็จ

_**Technology stack :** Python, Docker, Apache Airflow, Cloud Storage, BigQuery, Slack)_

## Project Overview
![Pipeline Overview](https://github.com/user-attachments/assets/ef352ab0-9fc7-49ea-aeee-61f6556d2e2e)

## Folder Structure and Explaination
```bash
etl-pipeline-with-airflow-in-docker/
├── assets/                           # โฟลเดอร์สำหรับเก็บรูปภาพต่างๆ ของโปรเจค
├── cred/                             # โฟลเดอร์สำหรับเก็บไฟล์ Credential ที่ connection กับ GCP
│   └── service-account-sbmk.json
│
├── dags/                             # โฟลเดอร์สำหรับเก็บไฟล์โค้ดของ DAGs ในการรัน Data pipeline บน Airflow
│   └── r2de2_workshop_sbmk.py
│
├── data/                             # โฟลเดอร์สำหรับเก็บไฟล์ข้อมูลที่ใช้สำหรับโปรเจค และผลลัพธ์จากการทำโปรเจค
│   ├── audible_data_merged.csv       # Raw data exreact from SQLite database (r2de2.db)
│   ├── audible_data_transformed.csv  # Final data
│   ├── conversion_rate.csv           # Raw data exreact from API
│   ├── r2de2_schema.json             # Schema field of final data
│   └── r2de2.db                      # Retail transaction data
│
├── logs/                             # โฟลเดอร์สำหรับเก็บ data logging บน Airflow
├── plugins/                          # โฟลเดอร์สำหรับ application ต่างๆที่ต้องการรันบน Airflow ผ่าน Dockerfile
│   ├── Dockerfile
│   └── requirements.txt
│
├── scripts/                          # โฟลเดอร์สำหรับเก็บไฟล์โค้ด python function เพิ่มเติมที่จะรันผ่าน Dags
│   └── slack_notify.py
│
├── .env                              # จัดการข้อมูลอยู่ในรูปตัวแปร ที่ต้องการเก็บเป็นความลับ
└── docker-compose.yaml               # Docker container ที่จะรัน Service แบบทีละหลายบน Airflow
```

### Data Pipeline Orchestration
![DAGs-Graph-2](https://github.com/user-attachments/assets/56e0a0b4-eb02-4e3e-b341-5ffeec9ecb7a)

### Cloud Storage
![GCS](https://github.com/user-attachments/assets/e1aef720-0b27-4340-91a4-acfc111ada5b)

### BigQuery
![BQ1](https://github.com/user-attachments/assets/4964c78f-fae6-43bc-a39e-5ec372ea2c05)

### Slack Notification
![Slack](https://github.com/user-attachments/assets/7f2684f7-a137-4551-9ece-a0d7b98974fb)

## Reference
* Road to Data Engineer 2.0 (2023) จาก [DataTH School](https://school.datath.com/)
* คุณบีท ปุณณ์สิริ บุณยเกียรติ Special Live หัวข้อเรื่อง [DataTH] Data Quality with Apache Airflow [GitHub](https://github.com/punsiriboo/data-quality-with-apache-airflow)
