import backtrader as bt
from backtrader.feeds import IBData
from datetime import time


class IntradayStrategy(bt.Strategy):
    params = (
        ('rsi_period', 14),
        ('bollinger_period', 20),
        ('devfactor', 2),
        ('risk_per_trade', 0.02),  # 2% per trade
        ('trail_percent', 1.0),  # 1% trailing stop
    )

    def __init__(self):
        # 技术指标
        self.rsi = bt.indicators.RSI(self.data.close,
                                     period=self.p.rsi_period)
        self.bollinger = bt.indicators.BollingerBands(
            self.data.close,
            period=self.p.bollinger_period,
            devfactor=self.p.devfactor
        )

        # 交易变量
        self.order = None
        self.position_size = 0
        self.stop_price = 0

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.stop_price = order.executed.price * (1 - self.p.trail_percent / 100)
            elif order.issell():
                self.stop_price = order.executed.price * (1 + self.p.trail_percent / 100)

    def next(self):
        # 只在交易时段内操作（根据标的物交易时间调整）
        if self.data.datetime.time() < time(9, 30) or \
                self.data.datetime.time() > time(15, 45):
            return

        # 关闭现有订单
        if self.order:
            return

        # 计算仓位大小
        self.position_size = (self.broker.getvalue() * self.p.risk_per_trade) / self.data.close[0]

        # 交易逻辑
        if not self.position:
            # 买入信号：价格突破布林带下轨且RSI < 30
            if self.data.close[0] < self.bollinger.lines.bot and self.rsi < 30:
                self.order = self.buy(size=self.position_size)
                self.stop_price = self.data.close[0] * 0.99  # 初始止损1%

            # 卖出信号：价格突破布林带上轨且RSI > 70
            elif self.data.close[0] > self.bollinger.lines.top and self.rsi > 70:
                self.order = self.sell(size=self.position_size)
                self.stop_price = self.data.close[0] * 1.01  # 初始止损1%

        # 持仓管理
        else:
            # 动态跟踪止损
            if self.position.size > 0:  # 多头
                self.stop_price = max(self.stop_price,
                                      self.data.close[0] * (1 - self.p.trail_percent / 100))
                if self.data.close[0] < self.stop_price:
                    self.close()
            elif self.position.size < 0:  # 空头
                self.stop_price = min(self.stop_price,
                                      self.data.close[0] * (1 + self.p.trail_percent / 100))
                if self.data.close[0] > self.stop_price:
                    self.close()

    def stop(self):
        print('策略结束')
        print(f'期末总资金: {self.broker.getvalue():.2f}')


# 配置IBKR连接
ibkr_account = 'YOUR_ACCOUNT_NUMBER'
api_host = '127.0.0.1'
api_port = 4001
code ='MSTR'

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
                  currency=contract['currency'],
                  timeframe=bt.TimeFrame.Ticks
                  )
    broker = bt.brokers.IBBroker(host=api_host, port=api_port, clientId=35)
    broker.start()
    cash2 = broker.getcash()

    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    cerebro.addstrategy(IntradayStrategy)

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

    print('初始资金: %.2f' % cerebro.broker.getvalue())
    # 运行策略
    results = cerebro.run()
    print('期末资金: {cerebro.broker.getvalue():.2f}')


    print(f'夏普比率: {results[0].analyzers.sharpe.get_analysis()["sharperatio"]:.2f}')
    print(f'最大回撤: {results[0].analyzers.drawdown.get_analysis()["max"]["drawdown"]:.2f}%')

    # 可视化
    cerebro.plot(style='candlestick')
