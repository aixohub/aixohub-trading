import threading
import time

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

from backtest.ibkr.logger_conf import setup_logging


class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def tickPrice(self, reqId, tickType, price, attrib):
        if tickType == 2 and reqId == 1:
            print('The current ask price is: ', price)


def run_loop():
    app.run()


app = IBapi()
app.connect('127.0.0.1', 7496, 58)


def run():
    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    time.sleep(1)  # Sleep interval to allow time for connection to server

    # Create contract object
    eurusd_contract = Contract()
    eurusd_contract.symbol = 'EUR'
    eurusd_contract.secType = 'CASH'
    eurusd_contract.exchange = 'IDEALPRO'
    eurusd_contract.currency = 'USD'

    # Request Market Data
    app.reqMktData(1, eurusd_contract, '', False, False, [])

    time.sleep(10)  # Sleep interval to allow time for incoming price data
    app.disconnect()


if __name__ == '__main__':
    setup_logging()
    run()
    time.sleep(1000)
