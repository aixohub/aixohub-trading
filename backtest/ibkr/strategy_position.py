import backtrader as bt
from backtest.ibkr.grids import SmaCross
from backtest.ibkr.grids.strategy_grid import GridStrategy
from backtrader import num2date
from backtrader.feeds import IBData
from btplotting import BacktraderPlottingLive, BacktraderPlotting

class IBKRPositionInitStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        ''' 日志记录函数 '''
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

    def __init__(self):
        pass

    def next(self):
        ts = self.data.datetime[0]
        date = num2date(self.data.datetime[0])
        symbol = self.data.contract.localSymbol
        print(
            f"""symbol: {symbol} time : {date} askPrice : {self.data.askPrice[0]}  bidPrice : {self.data.bidPrice[0]}  askSize : {self.data.askSize[0]} bidSize : {self.data.bidSize[0]}  """)
        # self.buy(data=self.data, symbol='nvda', size=1, price=100)
        # self.sell(data=self.data, symbol='nvda', size=1, price=100)
        # self.close()


api_port = 7496
# api_port = 4001

if __name__ == '__main__':
    broker = bt.brokers.IBBroker(host='127.0.0.1', port=api_port, clientId=35)
    broker.start()
    cash2 = broker.getcash()

    print(f"""cash2: {cash2}""")

    position = broker.getposition(symbol="TIGR")
    print(position)
    con = {
        "code": "USD.JPY",
        "secType": "CASH",
        "what": "BID_ASK",
        "exchange": "IDEALPRO",
        "currency": "USD",
    }

    contract1 = {
        "code": "USD.JPY",
        "secType": "CASH",
        "what": "BID_ASK",
        "exchange": "IDEALPRO",
        "currency": "USD",
    }

    contract = {
        "code": "TIGR",
        "secType": "STK",
        "what": "BID_ASK",
        "exchange": "SMART",
        "currency": "USD",
    }

    # 使用自定义数据源
    data = IBData(host='127.0.0.1', port=api_port, clientId=34,
                  name=contract['code'],
                  dataname=contract['code'],
                  secType=contract['secType'],
                  what=contract['what'],
                  exchange=contract['exchange'],
                  currency=contract['currency'],
                  timeframe=bt.TimeFrame.Ticks
                  )

    # 创建 Cerebro 引擎
    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    # 添加策略
    cerebro.addstrategy(SmaCross)

    # 使用 IBKR 作为 broker
    cerebro.setbroker(broker)

    # 运行策略
    cerebro.run()
    # 打印最终持仓和资金
    print(f'最终资金: {cerebro.broker.getvalue():.2f}')
