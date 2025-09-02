

import backtrader as bt

class KDJ(bt.Indicator):
    lines = ('k', 'd', 'j',)

    params = (
        ('period', 9),    # N周期
        ('k_period', 3),  # k值的周期
        ('d_period', 3),  # D值的周期

    )

    def __init__(self):
        # RSV = (close - low_n) / (high_n - low_n) * 100
        low_n = bt.ind.Lowest(self.data.low, period=self.p.period)
        high_n = bt.ind.Highest(self.data.high, period=self.p.period)
        rsv = (self.data.close - low_n) / (high_n - low_n) * 100

        # K, D 用 EMA 计算
        self.lines.k = bt.ind.EMA(rsv, period=self.p.k_period)
        self.lines.d = bt.ind.EMA(self.lines.k, period=self.p.d_period)
        self.lines.j = 3 * self.lines.k - 2 * self.lines.d
