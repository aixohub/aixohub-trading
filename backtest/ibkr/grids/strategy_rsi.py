import math

import backtrader as bt


class RsiStrategy(bt.Strategy):
    params = dict(
        smaperiod=3,
        short_period=5,
        long_period=13,
        stop_loss=0.02,
        period=8,
        short_ravg=5,
        long_ravg=13,
        max_position=10,
        spike_window=5,
        cls=0.5,
        csr=-0.1,
        clr=-0.3,
        open_percent=0.5,
        distance=2
    )

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # 是否有订单
        self.has_order = False
        # 记录上一次下单的价格
        self.last_order_price = None
        self.last_order_size = None
        self.open_percent = self.p.open_percent
        self.distance = self.p.distance
        self.rsi = bt.ind.RSI_Safe(self.data.close, period=self.p.period)
        self.long_RAVG = bt.indicators.SMA(self.data.close,
                                           period=self.p.long_ravg, plotname='Long Returns Avg')
        self.short_RAVG = bt.indicators.SMA(self.data.close,
                                            period=self.p.short_ravg, plotname='Short Returns Avg')

        # Long and Short Cross signal
        self.ls_cross = bt.indicators.CrossOver(self.long_RAVG, self.short_RAVG, plotname='LS crossover')
        self.ls_cross_SMA = bt.indicators.SMA(self.ls_cross,
                                              period=self.p.spike_window, plotname='LS_Spike')

        # Short and Close Cross signal
        self.sr_cross = bt.indicators.CrossOver(self.short_RAVG, self.data.close, plotname='SR crossover')
        self.sr_cross_SMA = bt.indicators.SMA(self.sr_cross,
                                              period=self.p.spike_window, plotname='SR_Spike')

        # Long and Close Cross signal
        self.lr_cross = bt.indicators.CrossOver(self.long_RAVG, self.data.close, plotname='LR crossover')
        self.lr_cross_SMA = bt.indicators.SMA(self.lr_cross,
                                              period=self.p.spike_window, plotname='LR_Spike')

        print('--------------------------------------------------')
        print('TestStrategy Created')
        print('--------------------------------------------------')

    def next(self):
        signal = self.p.cls * self.ls_cross + self.p.clr * self.lr_cross + self.p.csr * self.sr_cross
        current_price = self.data.close[0]
        print(f"signal==== {signal}")
        if signal > 0 and not self.has_order:
            buy_size = (self.broker.getvalue() / current_price) * self.open_percent
            buy_size = math.floor(buy_size)  # 调整为整数股数
            self.buy(price=current_price, size=buy_size)
            self.last_order_size = buy_size
            self.last_order_price = current_price
            self.has_order = True
            self.log(f'买入 {buy_size} 股，价格: {current_price}')
        elif signal < 0 and self.has_order:
            if current_price > self.last_order_price + self.distance:
                self.sell(price=current_price, size=self.last_order_size)
                self.has_order = False
                self.log(f'卖出 {self.last_order_size} 股，价格: {current_price}')
        # 输出当前持仓状态
        self.log(
            f'持仓规模: {self.getposition(self.data).size}, 市值: {self.broker.getvalue()}, 可用资金: {self.broker.getcash()}')

    def notify_order(self, order):
        order_status = ['Created', 'Submitted', 'Accepted', 'Partial', 'Completed', 'Canceled', 'Expired', 'Margin',
                        'Rejected']
        # 未被处理的订单
        if order.status in [order.Submitted, order.Accepted]:
            self.log('订单编号: %.0f ，标的名称: %s, 订单状态: %s' % (
                order.ref,
                order.data._name,
                order_status[order.status]
            ))

        # 已经处理的订单
        if order.status in [order.Partial, order.Completed]:
            if order.isbuy():
                self.log(
                    '买入， 订单状态: %s, 订单编号:%.0f, 标的名称: %s, 数量: %.2f, 成交价格: %.2f, 成交金额: %.2f, 费用（佣金等）: %.2f' % (
                        order_status[order.status]
                        , order.ref
                        , order.data._name
                        , order.executed.size
                        , order.executed.price
                        , order.executed.value
                        , order.executed.comm
                    )
                )
            else:
                self.log(
                    '卖出， 订单状态: %s, 订单编号:%.0f, 标的名称: %s, 数量: %.2f, 成交价格: %.2f, 成交金额: %.2f, 费用（佣金等）: %.2f' % (
                        order_status[order.status]
                        , order.ref
                        , order.data._name
                        , order.executed.size
                        , order.executed.price
                        , order.executed.value
                        , order.executed.comm
                    )
                )

        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            self.log('订单编号:%.0f, 标的名称: %s, 订单状态: %s' % (
                order.ref,
                order.data._name,
                order_status[order.status]
            ))

        self.order = None  # 重置订单状态

    def notify_trade(self, trade):
        pass
