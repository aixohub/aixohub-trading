# -*- coding: UTF-8 -*-
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import timedelta
from functools import partial
from typing import Callable, Sequence
from typing import TYPE_CHECKING

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.providers.apache.kafka.operators.produce import local_logger
from airflow.utils.module_loading import import_string
from dotenv import load_dotenv
from ibapi.contract import Contract
import queue
from datetime import datetime
from typing import Any

from airflow.hooks.base import BaseHook
from functools import cached_property

from ibapi.client import EClient
from ibapi.utils import floatMaxString, decimalMaxString
from ibapi.wrapper import EWrapper

logger = logging.getLogger(__name__)

load_dotenv()


class IBClient(EWrapper, EClient):
    def __init__(self, data_queue):
        EClient.__init__(self, self)
        self.data_queue = data_queue

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
        data = {
            "ReqId": reqId,
            "datetime": datetime.datetime.fromtimestamp(time).strftime("%Y-%m-%d %H:%M:%S"),
            "BidPrice": floatMaxString(bidPrice),
            "AskPrice": floatMaxString(askPrice),
            "BidSize": decimalMaxString(bidSize),
            "AskSize": decimalMaxString(askSize),
            "BidPastLow": tickAttribBidAsk.bidPastLow,
            "AskPastHigh": tickAttribBidAsk.askPastHigh
        }

        self.data_queue.put(data)


class IbApiHook(BaseHook):
    conn_name_attr = "ib_config_id"
    default_conn_name = "ib_default"
    conn_type = "ibapi"
    hook_name = "interactive brokers"

    def __init__(self, ib_config_id=default_conn_name, *args, **kwargs):
        """Initialize our Base."""
        super().__init__()
        self.ib_config_id = ib_config_id
        self.data_queue = queue.Queue()
        self.client = IBClient(data_queue=self.data_queue)

    @cached_property
    def get_conn(self) -> Any:
        """Get the configuration object."""
        config = self.get_connection(self.ib_config_id).extra_dejson
        host = config.get("ib_host") or '127.0.0.1'
        port = config.get("ib_port") or 7496
        client_id = config.get("ib_client_id") or 1

        if not (config.get("ib_host", None)):
            raise ValueError("config['ib_host'] must be provided.")

        self.client.connect(host, port, client_id)

        return self.client

    def close_conn(self):
        """
        Disconnects from the IB client if connected.
        """
        if self.client:
            self.client.disconnect()
            self.log.info("Disconnected from IB API")
            self.client = None

    def run_loop(self):
        self.client.run()


if TYPE_CHECKING:
    from airflow.utils.context import Context


def acked(err, msg):
    if err is not None:
        local_logger.error("Failed to deliver message: %s", err)
    else:
        local_logger.info(
            "Produced record to topic %s, partition [%s] @ offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


class Ib2KafkaOperator(BaseOperator):
    BLUE = "#ffefeb"
    ui_color = BLUE

    def __init__(
            self,
            topic: str,
            producer_function: str | Callable[..., Any],
            kafka_config_id: str = "kafka_default",
            producer_function_args: Sequence[Any] | None = None,
            producer_function_kwargs: dict[Any, Any] | None = None,
            delivery_callback: str | None = None,
            synchronous: bool = True,
            poll_timeout: float = 0,
            ib_config_id: str = "ib_default",
            ib_symbol: str | None = None,
            show_return_value_in_logs: bool = True,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if delivery_callback:
            dc = import_string(delivery_callback)
        else:
            dc = acked
        self.kafka_config_id = kafka_config_id
        self.topic = topic
        self.producer_function = producer_function
        self.producer_function_args = producer_function_args or ()
        self.producer_function_kwargs = producer_function_kwargs or {}
        self.delivery_callback = dc
        self.synchronous = synchronous
        self.poll_timeout = poll_timeout

        self.ib_config_id = ib_config_id
        self.ib_symbol = ib_symbol

        if not (self.topic and self.producer_function):
            raise AirflowException(
                "topic and producer_function must be provided. Got topic="
                f"{self.topic} and producer_function={self.producer_function}"
            )

        self.show_return_value_in_logs = show_return_value_in_logs

    def execute(self, context: Context) -> Any:
        producer = KafkaProducerHook(kafka_config_id=self.kafka_config_id).get_producer()
        if isinstance(self.producer_function, str):
            self.producer_function = import_string(self.producer_function)

        producer_callable = partial(
            self.producer_function,  # type: ignore
            *self.producer_function_args,
            **self.producer_function_kwargs,
        )

        ib_hook = IbApiHook(ib_config_id=self.ib_config_id)
        ib_client = ib_hook.get_conn()
        ib_client.nextorderId = None

        # Start the socket in a thread
        api_thread = threading.Thread(target=ib_client.run_loop(), daemon=True)
        api_thread.start()

        contract = Contract()
        contract.symbol = self.ib_symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        while True:
            if isinstance(ib_client.nextorderId, int):
                print('------connected------')
                break
            else:
                print('waiting for connection')
                time.sleep(1)
        ib_client.nextValidId(1)
        ib_client.reqTickByTickData(ib_client.nextorderId, contract, "BidAsk", 0, True)

        try:
            msg_count = 1
            while True:
                data = ib_hook.data_queue.get(timeout=10)  # 等待数据，超时时间可调
                self.process_data(data, msg_count, producer)
                msg_count = +1
                ib_hook.data_queue.task_done()
        except ib_hook.data_queue.Empty:
            self.log.info("No more data in queue; ending operation.")

        producer.flush()

    def process_data(self, data, msg_count, producer):
        """
        自定义数据处理逻辑，例如将数据保存到数据库、文件或其他系统
        """
        self.log.info(f"Processing data: {data}")
        data['symbol'] = self.ib_symbol
        producer.produce(self.topic, key="1", value=data, on_delivery=self.delivery_callback)
        producer.poll(self.poll_timeout)
        if msg_count % 20 == 0:
            producer.flush()


topic_stock = "stock-nasdaq"

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
