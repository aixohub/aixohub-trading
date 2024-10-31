# -*- coding: UTF-8 -*-
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timedelta, time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from confluent_kafka import Producer
from dotenv import load_dotenv
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.utils import floatMaxString, decimalMaxString
from ibapi.wrapper import EWrapper

logger = logging.getLogger(__name__)

load_dotenv()

symbol = "OKLO"
topic = "stock-nvda"
tableName = "stock_oklo"
# 创建生产者配置
conf = {
    'bootstrap.servers': 'www.aixohub.com:9092'  # Kafka 服务器地址

}


class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

        self.producer = Producer(conf)

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId
        print('The next valid order id is: ', self.nextorderId)

    def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice):
        print('orderStatus - orderid:', orderId, 'status:', status, 'filled', filled, 'remaining', remaining,
              'lastFillPrice', lastFillPrice)

    def openOrder(self, orderId, contract, order, orderState):
        print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange, ':', order.action,
              order.orderType, order.totalQuantity, orderState.status)

    def execDetails(self, reqId, contract, execution):
        print('Order Executed: ', reqId, contract.symbol, contract.secType, contract.currency, execution.execId,
              execution.orderId, execution.shares, execution.lastLiquidity)

    def tickByTickBidAsk(self, reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk):
        tickerId = reqId
        ts = datetime.datetime.fromtimestamp(time).strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""INSERT INTO {tableName} (symbol, datetime,  open, close, volume, bidPrice, bidSize, askPrice, askSize) VALUES
                ('{symbol}', '{ts}',{bidPrice},{askPrice},{bidSize},{bidPrice},{bidSize},{askPrice},{askSize}); """
        logger.info("BidAsk. ReqId:", reqId, "Time:",
                    datetime.datetime.fromtimestamp(time).strftime("%Y-%m-%d %H:%M:%S"),
                    "BidPrice:", floatMaxString(bidPrice), "AskPrice:", floatMaxString(askPrice), "BidSize:",
                    decimalMaxString(bidSize), "AskSize:", decimalMaxString(askSize),
                    "BidPastLow:", tickAttribBidAsk.bidPastLow, "AskPastHigh:", tickAttribBidAsk.askPastHigh)
        json_bytes = sql.encode('utf-8')
        self.producer.produce(topic, key='key344', value=json_bytes)


app = IBapi()


def run_loop():
    app.run()


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


def producer_function():
    app.connect('127.0.0.1', 4001, 12)

    app.nextorderId = None

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    while True:
        if isinstance(app.nextorderId, int):
            print('connected')
            print()
            break
        else:
            print('waiting for connection')
            time.sleep(1)
    app.nextValidId(1)
    app.reqTickByTickData(app.nextorderId, contract, "BidAsk", 0, True)
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

    send_to_kafka = ProduceToTopicOperator(
        task_id="send_to_kafka",
        kafka_config_id="t4",
        topic="stock-nvda",
        producer_function=producer_function,
        poll_timeout=10,
    )

    t6 = PythonOperator(task_id="close_connections", python_callable=hello_kafka)

    t0 >> send_to_kafka >> t6
