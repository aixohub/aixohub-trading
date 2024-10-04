import math

import backtrader as bt


class GridStrategy(bt.Strategy):
    """
    等差网格交易策略
    """
    params = (
        ('number', 10),  # 设置网格总数
        ('open_percent', 0.2),  # 初始仓位
        ('distance', 1),  # 设置网格间距
        ('base_price', 1)  # 设置初始价格
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
        # 定义网格的初始价格，根据当前价格开始划分网格
        self.base_price = self.p.base_price
        # 初始化状态
        self.open_flag = False
        self.last_index = 0
        self.per_size = 0
        self.max_index = 0
        self.min_index = 0

    def next(self):
        current_price = self.data.close[0]

        # 初始化订单：open_flag = False 表示首次买入
        if not self.open_flag:
            # 价格变化小于1%时开始初始化
            price_diff_percent = abs(current_price - self.base_price) / self.base_price
            if price_diff_percent < 0.01:
                # 根据资金和当前价格设置初始买入量
                buy_size = (self.broker.getvalue() / current_price) * self.p.open_percent
                buy_size = math.floor(buy_size)  # 调整为整数股数
                self.buy(price=current_price, size=buy_size)

                # 计算每个网格的买入卖出份额
                self.per_size = (self.broker.getvalue() / current_price) / self.p.number
                self.per_size = math.floor(self.per_size)  # 调整为整数股数
                self.max_index = round(self.p.number * self.p.open_percent)
                self.min_index = self.max_index - self.p.number

                self.last_index = 0
                self.open_flag = True  # 初始化完成
                self.has_order = True
                self.log(f'初始买入 {buy_size} 股，价格: {current_price}')

        else:
            # 计算当前价格所在的网格位置
            index = (current_price - self.base_price) // self.p.distance

            # 限制网格索引在最大和最小范围内
            index = max(min(index, self.max_index), self.min_index)

            # 计算网格的变化量
            change_index = index - self.last_index

            # 网格上移，卖出
            if change_index > 0 and self.has_order:
                self.sell(price=current_price, size=self.last_order_size)
                self.has_order = False
                self.log(f'卖出 {self.last_order_size} 股，价格: {current_price}')

            # 网格下移，买入
            elif change_index < 0 and not self.has_order:
                self.buy(price=current_price, size=self.per_size)
                self.has_order = True
                self.last_order_price = current_price
                self.last_order_size = self.per_size
                self.log(f'买入 { self.per_size} 股，价格: {current_price}')


            # 更新网格索引
            self.last_index = index

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
