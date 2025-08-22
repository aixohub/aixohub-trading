import threading
import time

from ibapi.client import EClient
from ibapi.common import SetOfString, SetOfFloat
from ibapi.contract import Contract, ContractDetails
from ibapi.ticktype import TickTypeEnum
from ibapi.utils import intMaxString
from ibapi.wrapper import EWrapper

from backtest.ibkr.logger_conf import setup_logging
from backtest.ibkr.sample.market_data import ib_port


class IBapi(EWrapper, EClient):
    def __init__(self, orderId):
        EClient.__init__(self, self)
        self.orderId = orderId

    def nextId(self):
        self.orderId += 2
        return self.orderId

    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderReject=""):
        print(f"error reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}, orderReject: {advancedOrderReject}")

    def tickPrice(self, reqId, tickType, price, attrib):
        print(f"tickPrice reqId: {reqId}, tickType: {TickTypeEnum.toStr(tickType)}, price: {price}, attrib: {attrib}")

    def tickSize(self, reqId, tickType, size):
        print(f"tickSize reqId: {reqId}, tickType: {TickTypeEnum.toStr(tickType)}, size: {size}")

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        print("contractDetails== ",reqId, contractDetails)

    def securityDefinitionOptionParameter(self, reqId: int, exchange: str,  underlyingConId: int,  tradingClass: str, multiplier: str, expirations: SetOfString, strikes: SetOfFloat):
        print("SecurityDefinitionOptionParameter.",   "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", intMaxString(underlyingConId), "TradingClass:", tradingClass,
              "Multiplier:", multiplier,  "Expirations:", expirations, "Strikes:", str(strikes))


def run_loop():
    app.run()


app = IBapi(orderId=1)
app.connect('127.0.0.1', ib_port, 58)


def run():
    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    time.sleep(1)  # Sleep interval to allow time for connection to server

    contract = Contract()
    contract.symbol = "NVDA"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    app.reqContractDetails(3, contract)

    app.reqSecDefOptParams(1, "TSLA", "", "STK", 76792991)
    time.sleep(20)
    # Create contract object
    # 定义期权合约
    contract = Contract()
    contract.symbol = "TSLA"
    contract.secType = "OPT"
    contract.exchange = "SMART"
    contract.currency = "USD"
    contract.lastTradeDateOrContractMonth = "20250905"  # 到期日
    contract.strike = 340
    contract.right = "C"
    contract.multiplier = "100"
    # Request Market Data
    app.reqMktData(2, contract, "", False, False, [])
    # app.reqMktData(app.nextId(), apple_contract, "232", False, False, [])

    time.sleep(20)  # Sleep interval to allow time for incoming price data
    app.disconnect()


if __name__ == '__main__':
    setup_logging()
    run()
    time.sleep(10000)
