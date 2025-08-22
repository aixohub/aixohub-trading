from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
import time


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        print("AccountSummary. ReqId:", reqId, "Account:", account, "Tag: ", tag, "Value:", value, "Currency:",
              currency)

    def accountSummaryEnd(self, reqId: int):
        print("AccountSummaryEnd. ReqId:", reqId)


if __name__ == '__main__':
    app = TradeApp()
    app.connect("127.0.0.1", 7496, clientId=11)

    time.sleep(1)

    app.reqAccountSummary(9001, "All", AccountSummaryTags.AllTags)
    app.run()
