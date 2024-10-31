# -*- coding: UTF-8 -*-
import logging
import os
from datetime import datetime, timedelta

import mysql.connector
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay
from confluent_kafka import Consumer
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定义全局变量以存储 Kafka 和 MySQL 连接
kafka_consumer = Consumer({
    'bootstrap.servers': 'www.aixohub.com:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})
load_dotenv()
mysql_host = os.environ.get("mysql_host")
mysql_user = os.environ.get("mysql_user")
mysql_passwd = os.environ.get("mysql_passwd")

mysql_database = os.environ.get("mysql_database")

mysql_connection = mysql.connector.connect(
    host=mysql_host,
    port=3306,
    user=mysql_user,
    password=mysql_passwd, database=mysql_database)

cursor = mysql_connection.cursor()

dag = DAG(
    'kafka_to_mysql_01',
    default_args=default_args,
    description='Kafka to MySQL DAG with global connections',
    schedule_interval='@daily',
    start_date=datetime(2024, 10, 29),
    tags=["ibkr", "backtrader"],
)


def init_env():
    global kafka_consumer, mysql_connection
    logger.info("init_env starting... ")


def run_task(**kwargs):
    logger.info("Backtrader starting... ")
    global kafka_consumer, mysql_connection

    stock_topic = os.environ.get("stock_topic")
    kafka_consumer.subscribe([stock_topic])
    while True:
        msg = kafka_consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        sql = msg.value().decode('utf-8')
        try:
            cursor.execute(sql)
            mysql_connection.commit()
        except:
            pass
        print('Received message: {}'.format(sql))


def close_env():
    global kafka_consumer, mysql_connection

    logger.info("Backtrader stopping...")
    if cursor:
        cursor.close()
    if mysql_connection:
        mysql_connection.close()
    if kafka_consumer:
        kafka_consumer.close()
    logger.info("Backtrader stopped...")




run_task = PythonOperator(
    task_id='run_task',
    python_callable=run_task,
    execution_timeout=timedelta(hours=8),
    trigger_rule='all_done',
    provide_context=True,
    dag=dag,
)

env_close = PythonOperator(
    task_id='env_close',
    python_callable=close_env,
    trigger_rule='all_done',
    provide_context=True,
    dag=dag,
)

run_task >> env_close
