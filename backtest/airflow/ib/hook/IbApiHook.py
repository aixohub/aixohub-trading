import queue
from datetime import datetime
from typing import Any

from airflow.hooks.base import BaseHook
from functools import cached_property

from ibapi.client import EClient
from ibapi.utils import floatMaxString, decimalMaxString
from ibapi.wrapper import EWrapper


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
            "Time": datetime.datetime.fromtimestamp(time).strftime("%Y-%m-%d %H:%M:%S"),
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
    conn_type = "IB api"
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
