import threading
import time

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.ticktype import TickTypeEnum
from ibapi.wrapper import EWrapper

from backtest.ibkr.logger_conf import setup_logging


class IBapi(EWrapper, EClient):
    def __init__(self, orderId):
        EClient.__init__(self, self)
        self.orderId = orderId

    def nextId(self):
        self.orderId += 1
        return self.orderId

    def error(self, reqId, errorCode, errorString, advancedOrderReject=""):
        print(f"error reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}, orderReject: {advancedOrderReject}")

    def tickPrice(self, reqId, tickType, price, attrib):
        print(f"tickPrice reqId: {reqId}, tickType: {TickTypeEnum.toStr(tickType)}, price: {price}, attrib: {attrib}")

    def tickSize(self, reqId, tickType, size):
        print(f"tickSize reqId: {reqId}, tickType: {TickTypeEnum.toStr(tickType)}, size: {size}")


def run_loop():
    app.run()


app = IBapi(orderId=1)
app.connect('127.0.0.1', 7496, 58)


def run():
    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    time.sleep(1)  # Sleep interval to allow time for connection to server

    # Create contract object
    apple_contract = Contract()
    apple_contract.symbol = 'AAPL'
    apple_contract.secType = 'STK'
    apple_contract.exchange = 'SMART'
    apple_contract.currency = 'USD'
    app.reqMarketDataType(3)
    # Request Market Data
    app.reqMktData(app.nextId(), apple_contract, "232", False, False, [])

    time.sleep(20)  # Sleep interval to allow time for incoming price data
    app.disconnect()


if __name__ == '__main__':
    setup_logging()
    run()
    time.sleep(1000)
