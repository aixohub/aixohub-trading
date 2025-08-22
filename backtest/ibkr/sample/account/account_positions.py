from ibapi.client import *
from ibapi.wrapper import *
import threading
import time


class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        EWrapper.__init__(self)

    def position(self, account: str, contract: Contract, position: Decimal, avgCost: float):
        print("Position.", "Account:", account, "Contract:", contract, "Position:", position, "Avg cost:", avgCost)

    def positionEnd(self):
        print("PositionEnd")


def websocket_con():
    app.run()


if __name__ == '__main__':
    app = TradingApp()
    app.connect("127.0.0.1", 7496, clientId=16)

    con_thread = threading.Thread(target=websocket_con, daemon=True)
    con_thread.start()
    time.sleep(3)

    app.reqPositions()
    time.sleep(20)
