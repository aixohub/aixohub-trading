import backtrader as bt
from backtrader.feeds import IBData
from datetime import time, datetime, timedelta

import backtrader as bt
import datetime


class VWAP(bt.Indicator):
    lines = ('vwap.py',)
    params = (('period', 0),)  # 0表示全天计算

    def __init__(self):
        self.addminperiod(1)  # 至少需要1个周期来开始计算
        self.cum_vol = bt.indicators.SumN(self.data.volume, period=self.p.period)
        self.cum_vol_price = bt.indicators.SumN(
            self.data.volume * (self.data.high + self.data.low + self.data.close) / 3, period=self.p.period)

    def next(self):
        if self.p.period == 0:
            # 每日重置
            if self.data.datetime.date() != self.data.datetime.date(-1):
                self.cum_vol[0] = self.data.volume[0]
                self.cum_vol_price[0] = (self.data.high[0] + self.data.low[0] + self.data.close[0]) / 3 * \
                                        self.data.volume[0]
            else:
                self.cum_vol[0] = self.cum_vol[-1] + self.data.volume[0]
                self.cum_vol_price[0] = self.cum_vol_price[-1] + (
                            self.data.high[0] + self.data.low[0] + self.data.close[0]) / 3 * self.data.volume[0]
        self.l.vwap[0] = self.cum_vol_price[0] / self.cum_vol[0]


class VWAPStrategy(bt.Strategy):
    params = (
        ('vwap_period', 0),  # 0表示全天VWAP
        ('trade_size', 100),  # 交易股数
        ('deviation_threshold', 0.005),  # 价格偏离阈值(0.5%)
    )

    def __init__(self):
        self.vwap = VWAP(self.data, period=self.p.vwap_period)
        self.order = None

    def next(self):
        if self.order:  # 检查是否有未完成订单
            return

        current_price = self.data.close[0]
        vwap_value = self.vwap.l.vwap[0]

        # 生成交易信号
        deviation = (current_price - vwap_value) / vwap_value

        if deviation < -self.p.deviation_threshold:
            # 价格低于VWAP阈值，买入
            self.buy(size=self.p.trade_size)
        elif deviation > self.p.deviation_threshold:
            # 价格高于VWAP阈值，卖出
            self.sell(size=self.p.trade_size)

    def notify_order(self, order):
        if order.status in [order.Completed]:
            # 订单完成时记录
            if order.isbuy():
                direction = '买入'
            else:
                direction = '卖出'
            print(f'{self.datetime.date()} | {direction}执行 | 价格：{order.executed.price:.2f}')
            self.order = None


# 配置IBKR连接
api_host = '127.0.0.1'
api_port = 4002
code = 'MSTR'

if __name__ == '__main__':
    # 添加IBKR数据源
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
                  currency=contract['currency']
                  )
    broker = bt.brokers.IBBroker(host=api_host, port=api_port, clientId=35)
    broker.start()
    cash2 = broker.getcash()

    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    cerebro.addstrategy(VWAPStrategy)

    # 设置交易参数
    cerebro.broker.setcash(cash2)
    cerebro.broker.setcommission(
        commission=0.0001,  # 0.01% 佣金
        margin=None,
        mult=1.0
    )

    # 添加滑点模拟
    cerebro.broker.set_slippage_perc(0.0005)  # 0.05% 滑点

    # 添加分析指标
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='ta')

    print('初始资金: %.2f' % cerebro.broker.getvalue())
    # 运行策略
    results = cerebro.run()
    print('期末资金: {cerebro.broker.getvalue():.2f}')

    print(f'夏普比率: {results[0].analyzers.sharpe.get_analysis()["sharperatio"]:.2f}')
    print(f'最大回撤: {results[0].analyzers.drawdown.get_analysis()["max"]["drawdown"]:.2f}%')


