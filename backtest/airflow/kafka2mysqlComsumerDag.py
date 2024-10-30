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

c = Consumer({
    'bootstrap.servers': 'www.aixohub.com:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'kafka_to_mysql_consumer',
        default_args=default_args,
        description='US market data save to mysql',
        schedule_interval='@daily',
        start_date=datetime(2024, 10, 29),
        tags=["ibkr", "backtrader"],
) as dag:
    def init_env(**kwargs):
        load_dotenv()
        mysql_host = os.environ.get("mysql_host")
        mysql_user = os.environ.get("mysql_user")
        mysql_passwd = os.environ.get("mysql_passwd")
        stock_topic = os.environ.get("stock_topic")
        mysql_database = os.environ.get("mysql_database")

        mysql_conn = mysql.connector.connect(
            host=mysql_host,
            port=3306,
            user=mysql_user,
            password=mysql_passwd, database=mysql_database)
        kwargs['ti'].xcom_push(key='mysql_conn', value=mysql_conn)
        kwargs['ti'].xcom_push(key='stock_topic', value=stock_topic)


    def run_task(**kwargs):
        logger.info("Backtrader starting... ")
        mysql_conn = kwargs['ti'].xcom_pull(key='mysql_conn', task_ids='env_init')
        stock_topic = kwargs['ti'].xcom_pull(key='stock_topic', task_ids='env_init')
        c.subscribe([stock_topic])
        cursor = mysql_conn.cursor()
        kwargs['ti'].xcom_push(key='cursor', value=stock_topic)

        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            sql = msg.value().decode('utf-8')
            try:
                cursor.execute(sql)
                mysql_conn.commit()
            except:
                pass
            print('Received message: {}'.format(sql))


    def close_env(**kwargs):
        logger.info("Backtrader stopping...")
        cursor = kwargs['ti'].xcom_pull(key='cursor', task_ids='env_init')
        mysql_conn = kwargs['ti'].xcom_pull(key='mysql_conn', task_ids='env_init')
        cursor.close()
        mysql_conn.close()
        c.close()
        logger.info("Backtrader stopped...")


    branch_workday = BranchDayOfWeekOperator(
        task_id="judge_workday",
        follow_task_ids_if_true="branch_true",
        follow_task_ids_if_false="branch_false",
        week_day={WeekDay.MONDAY, WeekDay.TUESDAY, WeekDay.WEDNESDAY, WeekDay.THURSDAY, WeekDay.FRIDAY},
    )

    env_init = PythonOperator(
        task_id='env_init',
        python_callable=init_env,
        provide_context=True,
    )

    run_task = PythonOperator(
        task_id='run_task',
        python_callable=run_task,
        execution_timeout=timedelta(hours=8),
        trigger_rule='all_done',
        provide_context=True,
    )

    env_close = PythonOperator(
        task_id='env_close',
        python_callable=close_env,
        provide_context=True,
    )

    weekend_task = EmptyOperator(task_id="branch_weekend")

    branch_workday >> [env_init >> run_task >> env_close, weekend_task]
