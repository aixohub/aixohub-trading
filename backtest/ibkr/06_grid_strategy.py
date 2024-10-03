import datetime
import math

import pandas as pd

import backtrader as bt


class GridStrategy(bt.Strategy):
    """
    网格交易
    """
    params = (
        ('number', 10),  # 设置网格总数
        ('open_percent', 0.5),  # 初始仓位 50%
        ('distance', 1),  # 设置网格间距
        ('base_price', 1)  # 设置初始价格
    )

    # 打印日志
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # 初始化订单状态
        self.open_flag = False
        self.last_index = 0
        self.per_size = 0
        self.max_index = 0
        self.min_index = 0
        self.order = None

    # 在next方法分为两个部分：建仓，和调仓。
    def next(self):
        # 判断是否首次买入（即初始化订单）
        # open_flag = True , 表示已经完成初始化网格交易
        if self.open_flag:
            index = (self.data.close[0] - self.p.base_price) // self.p.distance

            # 判断 网格是否 超过最大、最小值（破网）
            if index < self.min_index:
                index = self.min_index
            elif index > self.max_index:
                index = self.max_index

            # 计算网格变化:change_index 大于 0 则卖出，小于0 则买入
            change_index = index - self.last_index
            if change_index > 0:
                self.sell(data=self.data, size=change_index * self.per_size)
            elif change_index < 0:
                self.buy(data=self.data, size=change_index * self.per_size)

            # 更新前一个网格
            self.last_index = index

        # 入市条件：open_flag = False，滑点：涨跌幅 1%
        tmp_limit = math.fabs(self.data.close[0] - self.p.base_price) / self.p.base_price
        if not self.open_flag and tmp_limit < 0.01:
            # 买入：份额
            buy_size = self.broker.getvalue() / self.data.close[0] * self.p.open_percent // 100 * 100
            self.buy(data=self.data, size=buy_size)

            # 记录网格位置
            self.last_index = 0
            # 计算每个网格买入卖出的份额
            self.per_size = self.broker.getvalue() / self.data.close[0] // self.p.number // 100 * 100
            self.max_index = round(self.p.number * self.p.open_percent)
            self.min_index = self.max_index - self.p.number

            # 更新初始化订单状态
            self.open_flag = True

        self.log('持仓规模:{}, 市值:{}, 可用资金:{}'.format(
            self.getposition(self.data).size,
            self.broker.getvalue(),
            self.broker.getcash()
        ))

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


# 自定义交易费用（佣金、印花税、等等）
class ChStockCommission(bt.CommInfoBase):
    # 交易费用=佣金+印花税+过户费=净佣金+交易规费+印花税+过户费
    params = (
        ('miss5', False),  # 佣金少于5块时免不免5
        ('commission', 0.00015),  # 佣金费率万分之1.5
        ('stamp_duty', 0.0005),  # 印花税 万分之5
        ('transfer_fee', 0.00001),  # 过户费 十万分之1
        ('transaction_fee', 0.0000541),  # 交易规费
        ('percabs', True)  # 为True则使用小数，为False则使用百分数
    )

    def _getcommission(self, size, price, pseudoexec):
        # 买入
        if size > 0:
            fee = size * price * (self.p.commission +
                                  self.p.transfer_fee +
                                  self.p.transaction_fee)
        # 卖出
        elif size < 0:
            fee = size * price * (self.p.commission +
                                  self.p.transfer_fee +
                                  self.p.transaction_fee +
                                  self.p.stamp_duty
                                  )
        else:
            fee = 0

        # 是否免5
        if self.p.miss5:
            return fee
        else:
            if fee < 5:
                return 5
            return fee


class My_PandasData(bt.feeds.PandasData):
    params = (
        ('fromdate', datetime.datetime(2023, 5, 1)),
        ('todate', datetime.datetime(2024, 10, 1)),
        ('nullvalue', 0.0),
        ('datetime', 1),
        ('time', -1),
        ('high', 3),
        ('low', 4),
        ('open', 2),
        ('close', 5),
        ('volume', 6),
        ('openinterest', -1)
    )


import os, sys
from btplotting import BacktraderPlottingLive, BacktraderPlotting

date_formate_1 = "%Y-%m-%d %H:%M:%S"
date_formate_2 = "%Y%m%d"

if __name__ == '__main__':
    cerebro = bt.Cerebro()
    # 加载数据
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    datapath = os.path.join(modpath, 'datas/stock-NVDA.csv')
    datas = pd.read_csv(datapath)

    datas['datetime'] = pd.to_datetime(datas['datetime'], format=date_formate_1)
    datas = datas[['code', 'datetime', 'open', 'high', 'low', 'close', 'volume']].sort_values(by=['datetime'])

    datafeed = My_PandasData(dataname=datas)
    cerebro.adddata(datafeed, name='nvda')

    # 设置初始资金、佣金
    comminfo = ChStockCommission(
        miss5=True,
        commission=0.00015,
        stamp_duty=0,
        transfer_fee=0,
        transaction_fee=0
    )

    cerebro.broker.addcommissioninfo(comminfo)
    cerebro.broker.setcash(100000.0)

    # 加载策略
    cerebro.addstrategy(strategy=GridStrategy,
                        base_price=1.0,
                        distance=0.01)

    # 加载
    cerebro.addanalyzer(bt.analyzers.Returns, _name='收益')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer)
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='AnnualReturn')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.0, annualize=True, _name='SharpeRatio')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown')
    cerebro.addanalyzer(BacktraderPlottingLive, address='*', port=8899)

    # 开始回测
    cerebro.run()
    # p = BacktraderPlotting(style='bar', scheme=Tradimo())
    # p = BacktraderPlotting(style='bar', multiple_tabs=True)
    p = BacktraderPlotting(style='bar')
    cerebro.plot(p)
