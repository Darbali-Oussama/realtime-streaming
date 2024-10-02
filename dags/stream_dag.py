from datetime import datetime, timedelta
import time
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from cars_stream import create_kafka_producer, scrap
from stream_to_cassandra import store_to_cassandra

start_date = datetime(2024, 5, 12, 10, 12)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def task_scrap_data(**kwargs):
    data = scrap()
    kwargs['ti'].xcom_push(key='scraped_data', value=data)

def task_send_to_kafka(**kwargs):
    # Create Kafka producer here instead of XCom
    producer = create_kafka_producer()
    data = kwargs['ti'].xcom_pull(task_ids='scrap_data', key='scraped_data')
    
    if producer and data:
        end_time = time.time() + 20  # the script will run for 20 s
        while True:
            if time.time() > end_time:
                break
            producer.send("cars_data", json.dumps(data).encode('utf-8'))
            time.sleep(10)

def task_store_to_cassandra():
    """
    This task will trigger the store_to_cassandra function 
    to start streaming data from Kafka to Cassandra.
    """
    store_to_cassandra()

with DAG('cars_stream', default_args=default_args, schedule_interval='0 1 * * *', catchup=False) as dag:

    scrap_data_task = PythonOperator(
        task_id='scrap_data',
        python_callable=task_scrap_data,
        provide_context=True,
    )

    send_to_kafka_task = PythonOperator(
        task_id='send_to_kafka',
        python_callable=task_send_to_kafka,
        provide_context=True,
    )

    store_to_cassandra_task = PythonOperator(
        task_id='store_to_cassandra',
        python_callable=task_store_to_cassandra,
    )

    # Define task dependencies
    scrap_data_task >> send_to_kafka_task >> store_to_cassandra_task

