from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
import time


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def managedAccounts(self, accountsList: str):
        print("Account list:", accountsList)


if __name__ == '__main__':
    app = TradeApp()
    app.connect("127.0.0.1", 7496, clientId=10)

    time.sleep(1)

    app.reqManagedAccts()
    app.run()
