from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'spark_stream',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 13, 17, 0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "spark_stream",
    default_args=default_args,
    description="Stream data from Kafka to Cassandra",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1 
) as dag:

    spark_stream_task = SparkSubmitOperator(
        task_id="spark_streaming_task",
        application="/opt/airflow/scripts/spark_stream.py",
        conn_id="kafka_spark_cassandra",
        deploy_mode="client",
        packages=(
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        ),
        verbose=True,
    )
