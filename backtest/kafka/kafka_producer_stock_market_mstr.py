import datetime
import json
import threading
import time

from clickhouse_driver import Client
from confluent_kafka import Producer
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.order import *
from ibapi.wrapper import EWrapper

clickhouse_host = ''
clickhouse_user = "default"
clickhouse_pwd = ""
clickhouse_db = 'default'

symbol = "tsla"
topic = "stock-" + symbol
tableName = "stock_" + symbol
# 创建生产者配置
conf = {
    'bootstrap.servers': 'www.aixohub.com:9092'  # Kafka 服务器地址

}

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.clickHouseClient = Client(host=clickhouse_host, user=clickhouse_user, password=clickhouse_pwd,
                                       database=clickhouse_db)
        self.producer = Producer(conf)

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
        sql = f"""INSERT INTO {tableName} (symbol, datetime,  open, close, volume, bidPrice, bidSize, askPrice, askSize) VALUES
                ('{symbol}', '{ts}',{bidPrice},{askPrice},{bidSize},{bidPrice},{bidSize},{askPrice},{askSize}); """
        json_bytes = sql.encode('utf-8')

        print("BidAsk. ReqId:", reqId, "Time:", datetime.datetime.fromtimestamp(time).strftime("%Y-%m-%d %H:%M:%S"),
              "bidPrice:", floatMaxString(bidPrice), "AskPrice:", floatMaxString(askPrice), "BidSize:",
              decimalMaxString(bidSize), "AskSize:", decimalMaxString(askSize),
              "BidPastLow:", tickAttribBidAsk.bidPastLow, "AskPastHigh:", tickAttribBidAsk.askPastHigh)
        ticket_data = {
            'symbol': symbol,
            'time': datetime.datetime.fromtimestamp(time).strftime("%Y-%m-%d %H:%M:%S"),
            'bidPrice': floatMaxString(bidPrice),
            'askPrice': floatMaxString(askPrice),
            'bidSize': floatMaxString(bidSize),
            'askSize': floatMaxString(askSize)
        }
        ticket_data = json.dumps(ticket_data).encode('utf-8')
        # self.producer.produce(topic, key='key344', value=ticket_data)
        # self.producer.flush()





def run_loop():
    app.run()

    # Function to create FX Order contract


if __name__ == '__main__':
    app = IBapi()
    app.connect('127.0.0.1', 4002, 16)

    app.nextorderId = None

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    contract = Contract()
    contract.symbol = symbol
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

    time.sleep(36000)
    app.disconnect()
