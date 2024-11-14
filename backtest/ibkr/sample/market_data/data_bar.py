from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
import time


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def realtimeBar(self, reqId: TickerId, time: int, open_: float, high: float, low: float, close: float,
                    volume: Decimal, wap: Decimal, count: int):
        print("RealTimeBar. TickerId:", reqId, RealTimeBar(time, -1, open_, high, low, close, volume, wap, count))


if __name__ == '__main__':
    app = TradeApp()
    app.connect("127.0.0.1", 4001, clientId=16)

    contract = Contract()
    contract.symbol = "MSTR"
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = "SMART"

    app.reqRealTimeBars(3001, contract, 5, "TRADES", 0, [])

    app.run()
