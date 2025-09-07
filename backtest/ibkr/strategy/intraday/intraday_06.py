import logging



import backtrader as bt

from backtrader.feeds import IBData
from datetime import time

from backtrader.indicators import KDJ
from backtrader.stores import IBStore


class IntradayStrategy(bt.Strategy):
    params = (
        ('fast_ma', 5),  # 5周期快速均线
        ('slow_ma', 20),  # 20周期慢速均线

        # RSI参数
        ('rsi_period', 14),
        ('rsi_oversold', 30),
        ('rsi_overbought', 70),

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

        self.data1 = self.datas[0]  # 1分钟数据（主数据）


        # 均线交叉信号

        self.ema_fast1 = bt.ind.EMA(self.data.close, period=self.p.fast_ma)
        self.ema_slow1 = bt.ind.EMA(self.data.close, period=self.p.slow_ma)

        self.crossover1 = bt.indicators.CrossOver(self.ema_fast1, self.ema_slow1)

        self.rsi1 = bt.ind.RSI(self.data.close, period=self.params.rsi_period)


        if len(self.datas) > 2:
            self.data5 = self.datas[1]  # 5分钟数据
            self.ema_fast5 = bt.ind.EMA(self.data5.close, period=self.p.fast_ma)
            self.ema_slow5 = bt.ind.EMA(self.data5.close, period=self.p.slow_ma)
            self.crossover5 = bt.indicators.CrossOver(self.ema_fast5, self.ema_slow5)

        # 波动性测量
        self.atr = bt.indicators.ATR(self.data, period=self.p.atr_period)
        self.get_all_position()
        self.get_account_cash()



    def next(self):
        print(f"ema_fast1 { self.ema_fast1[0]}  ema_slow1 { self.ema_slow1[0]}  {self.data.close[0]} {self.crossover1 >0} {self.rsi1[0]} { self.atr[0]} ")
        # 交易逻辑
        if  self.in_trade_window:
            self.buy_bracket( size=1,  price=self.data.close[0] - 10, plimit=self.data.close[0] + 5,  limitprice=self.data.close[0] + 20,  stopprice=self.data.close[0] -30)
            self.in_trade_window = False

    def notify_order(self, order):
        print(f" notify_order {order}")



# 配置IBKR连接
ibkr_account = 'U15282766'
api_host = '127.0.0.1'
api_port = 4002
code ='tsla'

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
        "secType": "STK",
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
                  initAccountFlag= False,
                  timeframe=bt.TimeFrame.Minutes
                  )


    broker = bt.brokers.IBBroker(host=api_host, port=api_port, clientId=35, account =ibkr_account)
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
