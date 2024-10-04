import ibapi.contract

import backtrader as bt
from backtest.ibkr.logger_conf import setup_logging


class IBKRPositionInitStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        ''' 日志记录函数 '''
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

    def __init__(self):
        # 初始化策略时查询当前持仓
        self.init_position()

    def init_position(self):
        ''' 初始化 IBKR 持仓 '''
        for data in self.datas:
            position = self.broker.getposition(data)
            self.log(f'Symbol: {data._name}, 持仓: {position.size}, 成本价: {position.price:.2f}')

    def next(self):
        ''' 每个时间步都会调用，用于执行交易逻辑 '''
        # 示例：策略逻辑根据持仓执行操作
        for data in self.datas:
            position = self.broker.getposition(data)
            self.log(f'{data._name} 当前持仓: {position.size}')
            # 您可以根据自己的策略来决定是否加仓、平仓等


if __name__ == '__main__':
    setup_logging()

    broker = bt.brokers.IBBroker(host='127.0.0.1', port=7496, clientId=35)
    # 设置 IBKR 的连接参数
    store = bt.stores.IBStore(port=7496, clientId=123)
    store.start(broker=broker)
    print(" = * 5")

    cash1 = store.get_acc_cash()
    print(f"cash1 {cash1}")
    cash2 = broker.getcash()
    broker.orderstatus()
    contract = ibapi.contract.Contract()
    contract.conId = 12
    store.reqPositions()
    position = store.getposition(contract=contract, clone=False)

    # 创建 Cerebro 引擎
    cerebro = bt.Cerebro()
    data = store.getdata(dataname='USD.JPY', timeframe=bt.TimeFrame.Minutes)
    cerebro.adddata(data)
    # 添加策略
    cerebro.addstrategy(IBKRPositionInitStrategy)

    # 使用 IBKR 作为 broker
    cerebro.setbroker(broker)

    # 运行策略
    cerebro.run()

    # 打印最终持仓和资金
    print(f'最终资金: {cerebro.broker.getvalue():.2f}')
