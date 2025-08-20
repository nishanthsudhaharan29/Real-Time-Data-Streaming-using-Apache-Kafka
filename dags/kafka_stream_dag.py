from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append("/opt/airflow/scripts")
from kafka_stream import stream_data

default_args = {
    'owner': 'kafka_stream',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 13, 17, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "kafka_streaming",
    default_args= default_args,
    description="Stream data from API to kafka to cassandra",
    start_date=datetime(2025, 8, 13, 17, 0),
    schedule_interval= '@daily',
    catchup=False
) as dag:

    stream_to_kafka = PythonOperator(
        task_id='stream_to_kafka',
        python_callable=stream_data
    )


