import ibapi.contract

import backtrader as bt
from backtrader.feeds import IBData


class IBKRPositionInitStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        ''' 日志记录函数 '''
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

    def __init__(self):
        pass

    def next(self):
        print(f"""open : {self.data.open[0]}  close : {self.data.close[0]}""")
        self.buy(data=self.data, symbol='nvda', size=1, price=100)




if __name__ == '__main__':
    broker = bt.brokers.IBBroker(host='127.0.0.1', port=7496, clientId=35)
    broker.start()
    cash2 = broker.getcash()

    print(f"""cash2: {cash2}""")

    contract = ibapi.contract.Contract()
    contract.conId = 12
    position = broker.getposition(symbol="TIGR")
    print(position)
    code = 'USD.JPY'
    # 使用自定义数据源
    data = IBData(host='127.0.0.1', port=7496, clientId=34,
                  name=code,
                  dataname=code,
                  secType='CASH',
                  what='BID_ASK',
                  exchange="IDEALPRO",
                  currency='USD'
                  )

    # 创建 Cerebro 引擎
    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    # 添加策略
    cerebro.addstrategy(IBKRPositionInitStrategy)

    # 使用 IBKR 作为 broker
    cerebro.setbroker(broker)

    # 运行策略
    cerebro.run()

    # 打印最终持仓和资金
    print(f'最终资金: {cerebro.broker.getvalue():.2f}')
