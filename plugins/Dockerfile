FROM --platform=linux/amd64 apache/airflow:2.9.3

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir apache-airflow-providers-google==10.23.0 -r requirements.txt