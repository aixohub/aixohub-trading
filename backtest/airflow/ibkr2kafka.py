# -*- coding: UTF-8 -*-
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

from backtest.airflow.ib.ib_kafka_operator import Ib2KafkaOperator

logger = logging.getLogger(__name__)

load_dotenv()

topic_stock = "stock-nvda"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def load_connections():
    logger.info("load_connections %s", "...")
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

    db.merge_conn(
        Connection(
            conn_id="t6",
            conn_type="ibapi",
            extra=json.dumps(
                {
                    'ib_host': '127.0.0.1',
                    'ib_port': 4000,
                    'ib_client_id': '12',
                }
            ),
        )
    )


def producer_function():
    logger.info("producer_function ...")
    return


def hello_kafka():
    print("Hello Kafka !")
    return


with DAG(
        'ibkr_to_kafka',
        default_args=default_args,
        description='Stream IBKR market data to Kafka',
        schedule_interval='@daily',
        start_date=datetime(2024, 10, 30),
        tags=["ibkr", "backtrader"],
) as dag:
    t0 = PythonOperator(task_id="load_connections", python_callable=load_connections)

    send_to_kafka = Ib2KafkaOperator(
        task_id="send_to_kafka",
        kafka_config_id="t4",
        topic=topic_stock,
        producer_function=producer_function,
        poll_timeout=10,
        ib_config_id="t6",
        ib_symbol="NVDA",
    )

    t6 = PythonOperator(task_id="close_connections", python_callable=hello_kafka)

    t0 >> send_to_kafka >> t6
