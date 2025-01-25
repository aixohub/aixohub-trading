from airflow import DAG
from airflow.models import Variable
import logging

from airflow.operators.python import PythonOperator
from datetime import datetime
from confluent_kafka import Consumer
import mysql.connector
# 创建生产者配置
import json

logger = logging.getLogger(__name__)

# Kafka 配置
KAFKA_CONFIG = Variable.get("kafka_config", deserialize_json=True)

TOPIC = 'stock-soun'

# MySQL 配置
MYSQL_CONFIG = Variable.get("mysql_config", deserialize_json=True)


def consume_kafka_to_mysql():
    # 初始化 Kafka Consumer
    consumer = Consumer(KAFKA_CONFIG)
    logger.info(f"""kafka server {KAFKA_CONFIG["bootstrap.servers"]}""")

    consumer.subscribe([TOPIC])

    # 连接 MySQL
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = connection.cursor()

    try:
        while True:
            msg = consumer.poll(1.0)  # 等待 1 秒
            if msg is None:
                break  # 没有更多消息
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # 获取消息内容
            data = msg.value().decode('utf-8')
            v = json.loads(data)
            datetime = v['time']
            symbol = v['symbol']
            bidSize = v['bidSize']
            askSize = v['askSize']
            bidPrice = v['bidPrice']
            askPrice = v['askPrice']
            tableName = 'stock_{}'.format(symbol)
            sql = f"""INSERT INTO {tableName} (symbol, datetime,  open, close, volume, bid_price, bid_size, ask_price, ask_size) VALUES
                                               ('{symbol}', '{datetime}',{bidPrice},{askPrice},{bidSize},{bidPrice},{bidSize},{askPrice},{askSize}); """

            # 将数据写入 MySQL
            cursor.execute(sql)
            connection.commit()

            logger.info(f"Message written to MySQL: {data}")
    finally:
        consumer.close()
        cursor.close()
        connection.close()

# DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'kafka_to_mysql',
    default_args=default_args,
    description='Consume Kafka data and write to MySQL',
    schedule_interval=None,  # 设置触发模式
    start_date=datetime(2024, 12, 28),
    catchup=False,
)

consume_task = PythonOperator(
    task_id='consume_kafka_to_mysql',
    python_callable=consume_kafka_to_mysql,
    dag=dag,
)
