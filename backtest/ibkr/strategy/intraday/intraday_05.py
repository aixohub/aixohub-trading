import logging



import backtrader as bt

from backtrader.feeds import IBData
from datetime import time

from backtrader.stores import IBStore


class IntradayStrategy(bt.Strategy):
    params = (
        ('fast_ma', 5),  # 5周期快速均线
        ('slow_ma', 20),  # 20周期慢速均线
        ('atr_period', 14),  # ATR周期
        ('risk_percent', 1),  # 单笔风险百分比
        ('profit_target', 2),  # 止盈比例 (相对于止损)
        ('trade_start', time(9, 30)),  # 美股交易时间
        ('trade_end', time(15, 55)),
    )

    def __init__(self):
        # 日内时间过滤器
        self.in_trade_window = True
        self.has_order = True

        # 均线交叉信号
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.p.fast_ma)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.p.slow_ma)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

        # 波动性测量
        self.atr = bt.indicators.ATR(self.data, period=self.p.atr_period)
        self.get_all_position()
        self.get_account_cash()



    def next(self):
        print(f"fast_ma {self.crossover} ")
        # 交易逻辑
        if  self.in_trade_window:
            self.buy(size=1, price=0.01, plimit =0.01,  exectype= bt.Order.Limit)
            self.in_trade_window = False

    def notify_order(self, order):
        print(f" notify_order {order}")



# 配置IBKR连接
ibkr_account = 'YOUR_ACCOUNT_NUMBER'
api_host = '127.0.0.1'
api_port = 4002
code ='TSLA'

if __name__ == '__main__':

    # 添加IBKR数据源
    # contract = {
    #     "code": code,
    #     "secType": "STK",
    #     "what": "BID_ASK",
    #     "exchange": "SMART",
    #     "currency": "USD",
    # }

    contract = {
        "code": code,
        "secType": "OPT",
        "what": "BID_ASK",
        "exchange": "SMART",
        "currency": "USD",
    }

    data = IBData(host=api_host, port=api_port, clientId=20,
                  name=contract['code'],
                  dataname=contract['code'],
                  secType=contract['secType'],
                  what=contract['what'],
                  exchange=contract['exchange'],
                  currency=contract['currency'],
                  strike=230,
                  right='C',
                  expiry='20250829',
                  initAccountFlag= False,
                  timeframe=bt.TimeFrame.Minutes
                  )


    broker = bt.brokers.IBBroker(host=api_host, port=api_port, clientId=35, account ="U15282766")
    broker.start()
    cerebro = bt.Cerebro()
    cerebro.setbroker(broker)

    cerebro.adddata(data)
    cerebro.addstrategy(IntradayStrategy)


    # 添加分析指标
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

    # 运行策略
    results = cerebro.run()

    # 输出结果
    print(f'最终资金: {cerebro.broker.getvalue():.2f}')
    print(f'夏普比率: {results[0].analyzers.sharpe.get_analysis()["sharperatio"]:.2f}')
    print(f'最大回撤: {results[0].analyzers.drawdown.get_analysis()["max"]["drawdown"]:.2f}%')

    # 可视化
    cerebro.plot(style='candlestick')
