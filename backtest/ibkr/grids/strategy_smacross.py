import math

import backtrader as bt


class SmaCross(bt.Strategy):
    # list of parameters which are configurable for the strategy
    params = dict(
        open_percent=0.5,
        pfast=10,  # period for the fast moving average
        pslow=30  # period for the slow moving average
    )

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        sma1 = bt.ind.SMA(period=self.p.pfast)  # fast moving average
        sma2 = bt.ind.SMA(period=self.p.pslow)  # slow moving average
        self.crossover = bt.ind.CrossOver(sma1, sma2)  # crossover signal
        self.signal_add(bt.SIGNAL_LONG, self.crossover)  # use it as LONG signal

        # 记录上一次下单的价格
        self.last_order_price = None
        self.last_order_size = None
        self.open_percent = self.p.open_percent

    def next(self):
        current_price = self.data.close[0]

        if not self.position:  # not in the market
            if self.crossover > 0:  # if fast crosses slow to the upside
                buy_size = (self.broker.getvalue() / current_price) * self.open_percent
                buy_size = math.floor(buy_size)  # 调整为整数股数
                self.buy(price=current_price, size=buy_size)  # enter long
                self.last_order_size = buy_size
                self.last_order_price = current_price
                self.log(f'买入 {buy_size} 股，价格: {current_price}')
                self.log(
                    f'持仓规模: {self.getposition(self.data).size}, 市值: {self.broker.getvalue()}, 可用资金: {self.broker.getcash()}')

        elif self.crossover < 0:  # in the market & cross to the downside
            self.close()  # close long position
            self.log(f'卖出 {self.last_order_size} 股，价格: {current_price}')
            self.log(
                f'持仓规模: {self.getposition(self.data).size}, 市值: {self.broker.getvalue()}, 可用资金: {self.broker.getcash()}')
