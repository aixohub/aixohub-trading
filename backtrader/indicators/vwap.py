import backtrader as bt

class VWAP(bt.Indicator):
    '''
    成交量加权平均价 (Volume Weighted Average Price)
    在每个交易日重新开始计算
    '''
    lines = ('vwap',)
    plotinfo = dict(plot=True, subplot=False)

    def __init__(self):
        self.addminperiod(1)  # 至少需要1个周期
        self.current_date = None
        self.cumulative_typ = 0.0  # 累计典型价格*成交量
        self.cumulative_volume = 0.0  # 累计成交量

    def next(self):
        # 获取当前日期
        current_dt = self.data.datetime.date(0)

        # 如果是新交易日，重置累计值
        if current_dt != self.current_date:
            self.current_date = current_dt
            self.cumulative_typ = 0.0
            self.cumulative_volume = 0.0

        # 计算典型价格 (高+低+收)/3
        typical_price = (self.data.high[0] + self.data.low[0] + self.data.close[0]) / 3.0

        # 更新累计值
        self.cumulative_typ += typical_price * self.data.volume[0]
        self.cumulative_volume += self.data.volume[0]

        # 计算VWAP
        if self.cumulative_volume > 0:
            self.lines.vwap[0] = self.cumulative_typ / self.cumulative_volume
        else:
            self.lines.vwap[0] = typical_price  # 避免除零错误