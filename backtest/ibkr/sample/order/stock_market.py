import datetime
import threading
import time

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.order import *
from ibapi.wrapper import EWrapper


class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

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
        print("BidAsk. ReqId:", reqId, "Time:", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d-%H:%M:%S"),
              "BidPrice:", floatMaxString(bidPrice), "AskPrice:", floatMaxString(askPrice), "BidSize:",
              decimalMaxString(bidSize), "AskSize:", decimalMaxString(askSize),
              "BidPastLow:", tickAttribBidAsk.bidPastLow, "AskPastHigh:", tickAttribBidAsk.askPastHigh)


def run_loop():
    app.run()

    # Function to create FX Order contract


if __name__ == '__main__':
    app = IBapi()
    app.connect('127.0.0.1', 4001, 12)

    app.nextorderId = None

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    contract1 = {
        "conId": "12",
        "symbol": "NVDA",
        "secType": 'STK',
        "exchange": 'SMART',
        "currency": 'USD'
    }
    contract = Contract()
    contract.symbol = "NVDA"
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
    # Check if the API is connected via orderid

    time.sleep(20)
    app.disconnect()
