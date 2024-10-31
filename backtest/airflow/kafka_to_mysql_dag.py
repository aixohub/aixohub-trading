# -*- coding: UTF-8 -*-
from __future__ import annotations

import functools
import json
import logging
import os
from datetime import datetime, timedelta

import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()
mysql_host = os.environ.get("mysql_host")
mysql_user = os.environ.get("mysql_user")
mysql_passwd = os.environ.get("mysql_passwd")
stock_topic = os.environ.get("stock_topic")
mysql_database = os.environ.get("mysql_database")

mysql_connection = mysql.connector.connect(
    host=mysql_host,
    port=3306,
    user=mysql_user,
    password=mysql_passwd, database=mysql_database)
cursor = mysql_connection.cursor()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def load_connections():
    logger.info("load_connections %s", "...")
    logger.info("model-path %s", get_module())
    from airflow.models import Connection
    from airflow.utils import db
    db.merge_conn(
        Connection(
            conn_id="t4",
            conn_type="kafka",
            extra=json.dumps(
                {
                    'bootstrap.servers': 'www.aixohub.com:9092',
                    'group.id': 't4',
                    'auto.offset.reset': 'earliest',
                    "enable.auto.commit": False,
                }
            ),
        )
    )


def consumer_function(message, prefix=None):
    key = message.key()
    value = message.value().decode('utf-8')
    logger.info("%s %s @ %s; %s : %s", prefix, message.topic(), message.offset(), key, value)
    return


def consumer_function_batch(messages, prefix=None):
    for message in messages:
        key = message.key()
        value = message.value().decode('utf-8')
        v = json.loads(value)
        datetime = v['datetime']
        symbol = v['symbol']
        bidSize = v['bidSize']
        askSize = v['askSize']
        bidPrice = v['bidPrice']
        askPrice = v['askPrice']
        tableName = 'stock_{}'.format(symbol)
        sql = f"""INSERT INTO {tableName} (symbol, datetime,  open, close, volume, bidPrice, bidSize, askPrice, askSize) VALUES
                        ('{symbol}', '{datetime}',{bidPrice},{askPrice},{bidSize},{bidPrice},{bidSize},{askPrice},{askSize}); """
        try:
            cursor.execute(sql)
            mysql_connection.commit()
        except:
            logger.info("consumer_function_batch error ")
            pass
        logger.info("%s %s @ %s; %s : %s", prefix, message.topic(), message.offset(), key, value)


def await_function(message, prefix=None):
    key = message.key()
    value = message.value().decode('utf-8')
    logger.info("%s %s @ %s; %s : %s", prefix, message.topic(), message.offset(), key, value)


def wait_for_event(message, **context):
    key = message.key()
    value = message.value().decode('utf-8')
    logger.info(" %s @ %s; %s : %s", message.topic(), message.offset(), key, value)


def hello_kafka():
    print("Hello Kafka !")
    if cursor:
        cursor.close()
    if mysql_connection:
        mysql_connection.close()
    return


def get_module():
    import importlib
    module = importlib.import_module(__name__)
    return module.__name__


with DAG(
        'kafka_to_mysql_05',
        default_args=default_args,
        description='US market data save to mysql',
        schedule_interval='@daily',
        start_date=datetime(2024, 10, 30),
        tags=["ibkr", "backtrader"],
) as dag:
    t0 = PythonOperator(task_id="load_connections", python_callable=load_connections)

    t4b = ConsumeFromTopicOperator(
        kafka_config_id="t4",
        task_id="consume_from_topic_2_b",
        topics=["stock-nasdaq"],
        apply_function_batch=functools.partial(consumer_function_batch, prefix="consumed:::"),
        poll_timeout=300,
        max_batch_size=100,
    )


    t6 = PythonOperator(task_id="hello_kafka", python_callable=hello_kafka)

    t0 >> [t4b]  >> t6
