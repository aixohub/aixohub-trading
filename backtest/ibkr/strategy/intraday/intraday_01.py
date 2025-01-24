import backtrader as bt
from backtrader.feeds import IBData
from datetime import time


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
        self.in_trade_window = False

        # 均线交叉信号
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.p.fast_ma)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.p.slow_ma)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

        # 波动性测量
        self.atr = bt.indicators.ATR(self.data, period=self.p.atr_period)

    def next(self):
        # 检查是否在交易时段内
        self.in_trade_window = self.data.datetime.time() >= self.p.trade_start and \
                               self.data.datetime.time() <= self.p.trade_end

        if not self.in_trade_window:
            self.close_all_positions()
            return

        # 计算仓位大小
        position_size = self.calculate_position_size()

        # 交易逻辑
        if not self.position:
            if self.crossover > 0:  # 金叉
                self.buy(size=position_size)
                self.stop_loss = self.data.close[0] - 2 * self.atr[0]
                self.take_profit = self.data.close[0] + 3 * self.atr[0]
            elif self.crossover < 0:  # 死叉
                self.sell(size=position_size)
                self.stop_loss = self.data.close[0] + 2 * self.atr[0]
                self.take_profit = self.data.close[0] - 3 * self.atr[0]
        else:
            # 更新止损止盈
            if self.position.size > 0:
                if self.data.close[0] <= self.stop_loss or self.data.close[0] >= self.take_profit:
                    self.close()
            elif self.position.size < 0:
                if self.data.close[0] >= self.stop_loss or self.data.close[0] <= self.take_profit:
                    self.close()

    def calculate_position_size(self):
        account_value = self.broker.getvalue()
        risk_amount = account_value * self.p.risk_percent / 100
        position_size = risk_amount / (2 * self.atr[0])
        return int(position_size // 1)  # 向下取整

    def close_all_positions(self):
        if self.position:
            self.close()

    def notify_timer(self, timer, when, *args, **kwargs):
        # 在收盘前5分钟强制平仓
        if self.data.datetime.time() >= time(15, 55):
            self.close_all_positions()


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

    # 运行策略
    results = cerebro.run()

    # 输出结果
    print(f'最终资金: {cerebro.broker.getvalue():.2f}')
    print(f'夏普比率: {results[0].analyzers.sharpe.get_analysis()["sharperatio"]:.2f}')
    print(f'最大回撤: {results[0].analyzers.drawdown.get_analysis()["max"]["drawdown"]:.2f}%')

    # 可视化
    cerebro.plot(style='candlestick')
