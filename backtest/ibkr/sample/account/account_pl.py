from ibapi.client import *
from ibapi.wrapper import *
import time

from ibapi.client import *
from ibapi.wrapper import *
import time


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def pnl(self, reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float):
        print("Daily PnL. ReqId:", reqId, "DailyPnL:", dailyPnL, "UnrealizedPnL:", unrealizedPnL, "RealizedPnL:",
              realizedPnL)




if __name__ == '__main__':
    app = TradeApp()
    app.connect("127.0.0.1", 7496, clientId=13)

    time.sleep(1)
    app.reqPnL(102, "U12081371", "")

    app.run()
