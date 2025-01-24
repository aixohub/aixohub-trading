import backtrader as bt
from backtrader.feeds import IBData
from datetime import time, datetime, timedelta


class MomentumIntradayStrategy(bt.Strategy):
    params = (
        ('open_duration', 30),  # 开盘观察期（分钟）
        ('rsi_period', 14),  # RSI周期
        ('atr_period', 14),  # ATR周期
        ('atr_multiplier', 1.5),  # ATR止损倍数
        ('risk_percent', 1),  # 单笔风险比例
        ('profit_ratio', 2),  # 盈亏比
        ('trade_start', time(9, 30)),
        ('trade_end', time(15, 55)),
    )

    def __init__(self):
        # 时间控制
        self.start_bar = None
        self.in_trading_window = False

        # 动量指标
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.rsi_period)
        self.atr = bt.indicators.ATR(self.data, period=self.p.atr_period)

        # 开盘区间跟踪
        self.start_time = self.data.datetime.time()
        self.high_channel = self.data.high[0]
        self.low_channel = self.data.low[0]

    def next(self):
        # 时间过滤
        current_time = self.data.datetime.time()
        self.in_trading_window = self.p.trade_start <= current_time <= self.p.trade_end

        if not self.in_trading_window:
            self.close_all()
            return

        # 开盘后30分钟建立通道
        if current_time <= self.add_time(self.p.trade_start, minutes=self.p.open_duration):
            self.high_channel = max(self.high_channel, self.data.high[0])
            self.low_channel = min(self.low_channel, self.data.low[0])
            return
        else:
            # 生成交易信号
            self.generate_signals()

    def generate_signals(self):
        if self.position:
            self.check_exit_conditions()
            return

        # 多头信号：突破通道高点且RSI>50
        if self.data.close[0] > self.high_channel and self.rsi[0] > 50:
            risk = self.data.close[0] - (self.data.close[0] - 2 * self.atr[0])
            position_size = self.calc_position_size(risk)
            self.buy(size=position_size)
            self.stop_price = self.data.close[0] - 2 * self.atr[0]
            self.target_price = self.data.close[0] + 3 * self.atr[0]

        # 空头信号：跌破通道低点且RSI<50
        elif self.data.close[0] < self.low_channel and self.rsi[0] < 50:
            risk = (self.data.close[0] + 2 * self.atr[0]) - self.data.close[0]
            position_size = self.calc_position_size(risk)
            self.sell(size=position_size)
            self.stop_price = self.data.close[0] + 2 * self.atr[0]
            self.target_price = self.data.close[0] - 3 * self.atr[0]

    def check_exit_conditions(self):
        if self.position.size > 0:
            if self.data.close[0] >= self.target_price or \
                    self.data.close[0] <= self.stop_price:
                self.close()
        elif self.position.size < 0:
            if self.data.close[0] <= self.target_price or \
                    self.data.close[0] >= self.stop_price:
                self.close()

    def calc_position_size(self, risk):
        account_value = self.broker.getvalue()
        risk_amount = account_value * self.p.risk_percent / 100
        return int(risk_amount / risk)

    def close_all(self):
        if self.position:
            self.close()

    def add_time(self, t, minutes):
        new_time = (datetime.datetime.combine(datetime.date.today(), t) +
                    timedelta(minutes=minutes)).time()
        return new_time


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
    cerebro.addstrategy(MomentumIntradayStrategy)

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
    print(strat.analyzers.ta.get_analysis())


    # 可视化
    cerebro.plot(style='candlestick')
