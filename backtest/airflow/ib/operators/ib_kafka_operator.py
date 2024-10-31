from __future__ import annotations

import threading
import time
from functools import partial
from typing import Any, Callable, Sequence
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.providers.apache.kafka.operators.produce import local_logger
from airflow.utils.module_loading import import_string
from ibapi.contract import Contract

from backtest.airflow.ib.hook.IbApiHook import IbApiHook

if TYPE_CHECKING:
    from airflow.utils.context import Context, context_merge


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
            ib_config_id: str = "ibkr_default",
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
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        while True:
            if isinstance(ib_client.nextorderId, int):
                print('connected')
                print()
                break
            else:
                print('waiting for connection')
                time.sleep(1)
        ib_client.nextValidId(1)
        ib_client.reqTickByTickData(ib_client.nextorderId, contract, "BidAsk", 0, True)

        # For each returned k/v in the callable : publish and flush if needed.
        for k, v in producer_callable():
            producer.produce(self.topic, key=k, value=v, on_delivery=self.delivery_callback)
            producer.poll(self.poll_timeout)
            if self.synchronous:
                producer.flush()

        producer.flush()
