import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime, time


class IntradayStrategy(bt.Strategy):
    params = (
        # 均线参数
        ('ema_fast_period', 7),
        ('ema_slow_period', 15),
        # RSI参数
        ('rsi_period', 11),
        ('rsi_oversold', 21),
        ('rsi_overbought', 80),
        # 风险管理参数
        ('stop_loss', 0.02),  # 2% 止损
        ('trail_stop', False),  # 是否使用追踪止损
        ('trail_percent', 0.01),  # 1% 追踪止损
        ('position_size', 0.95),  # 使用95%的资金
    )

    def __init__(self):
        # 保存订单引用
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # 初始化指标
        self.ema_fast = bt.ind.EMA(self.data.close, period=self.params.ema_fast_period)
        self.ema_slow = bt.ind.EMA(self.data.close, period=self.params.ema_slow_period)

        # RSI指标
        self.rsi = bt.ind.RSI(self.data.close, period=self.params.rsi_period)

        self.rsi_ema = bt.indicators.EMA( self.rsi, period=5)

        # VWAP - 日内重要指标
        self.vwap = bt.ind.VWAP(self.data)

        # 跟踪最高价用于追踪止损
        self.high_since_entry = -np.inf

        self.has_position = False
        self.position_size = 0
        # 记录交易
        self.trades = []

    def next(self):
        # 如果已有订单，等待执行完成
        if self.order:
            return

        # 检查是否持有头寸
        if not self.has_position:
            # 没有头寸，寻找买入机会

            # 买入条件：
            # 1. 快线上穿慢线（金叉）
            # 2. RSI从超卖区回升（<30）
            # 3. 价格在VWAP之上（多头趋势）
            if (self.rsi[0] < self.params.rsi_oversold and
                    self.data.close[0] > self.vwap[0]):
                # 计算下单数量（使用95%的资金）
                size = self.broker.getcash() * self.params.position_size / self.data.close[0]
                self.position_size = int(size / 100) * 100  # 取整百股

                self.log(f'BUY CREATE, Size: {self.position_size}, Price: {self.data.close[0]:.2f}  rsi: {self.rsi[0]}')
                self.order = self.buy(size=self.position_size, price=self.data.close[0])

        else:
            # 已经持有头寸，检查卖出条件

            # 更新最高价（用于追踪止损）
            if self.data.close[0] > self.high_since_entry:
                self.high_since_entry = self.data.close[0]



            # 卖出条件2：RSI超买
            sell_signal2 = self.rsi[0] > self.params.rsi_overbought

            # 卖出条件3：价格跌破VWAP
            sell_signal3 = self.data.close[0] < self.vwap[0]

            # 卖出条件4：止损
            current_stop_loss = self.buyprice * (1 - self.params.stop_loss)
            stop_loss_hit = self.data.close[0] < current_stop_loss

            # 卖出条件5：追踪止损
            trail_stop_price = self.high_since_entry * (1 - self.params.trail_percent)
            trail_stop_hit = self.params.trail_stop and self.data.close[0] < trail_stop_price

            # 任何一个卖出条件触发就平仓
            if sell_signal2 or sell_signal3 or stop_loss_hit or trail_stop_hit:
                reason = []
                if sell_signal2: reason.append("RSI超买")
                if sell_signal3: reason.append("跌破VWAP")
                if stop_loss_hit: reason.append(f"止损({self.params.stop_loss * 100}%)")
                if trail_stop_hit: reason.append("追踪止损")

                # self.log(f'SELL CREATE, Reason: {", ".join(reason)}, Price: {self.data.close[0]:.2f}')
                self.order = self.sell(size=self.position_size, price=self.data.close[0])

    def log(self, txt, dt=None):
        ''' 日志函数 '''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 订单已提交/被接受 - 无需操作
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY  EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
                self.high_since_entry = self.data.close[0]  # 设置入场后的最高价
                self.has_position = True
            else:  # Sell
                self.has_position = False
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))
                # 记录交易详情
                trade_profit = order.executed.value - (self.buyprice * order.executed.size)
                self.trades.append({
                    'entry_date': self.datas[0].datetime.date(0),
                    'exit_date': self.datas[0].datetime.date(0),
                    'entry_price': self.buyprice,
                    'exit_price': order.executed.price,
                    'size': order.executed.size,
                    'profit': trade_profit,
                    'return_pct': (order.executed.price / self.buyprice - 1) * 100
                })

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # 重置订单
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def stop(self):
        ''' 回测结束时的分析 '''
        self.log('回测结束')
        if self.trades:
            total_profit = sum(trade['profit'] for trade in self.trades)
            total_return = sum(trade['return_pct'] for trade in self.trades)
            win_trades = [t for t in self.trades if t['profit'] > 0]
            win_rate = len(win_trades) / len(self.trades) * 100

            self.log(f'总交易次数: {len(self.trades)}')
            self.log(f'总利润: {total_profit:.2f}')
            self.log(f'总收益率: {total_return:.2f}%')
            self.log(f'胜率: {win_rate:.2f}%')
            self.log(f'平均每笔收益: {total_return / len(self.trades):.2f}%')


# 数据预处理函数 - 确保数据格式正确
def prepare_intraday_data(csv_path):
    ''' 加载并预处理日内数据 '''
    df = pd.read_csv(csv_path)

    # 确保日期时间格式正确
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)

    # 确保数据列名正确
    df = df[['open', 'high', 'low', 'close', 'volume']]

    # 转换为backtrader数据格式
    data = bt.feeds.PandasData(
        dataname=df,
        timeframe=bt.TimeFrame.Minutes,  # 分钟线数据
        compression=1,  # 5分钟线
        fromdate=datetime(2024, 1, 1),
        todate=datetime(2024, 12, 31)
    )
    return data


# 主函数
def run_backtest():
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()

    # 添加策略
    cerebro.addstrategy(IntradayStrategy)

    # 加载数据 - 请替换为您的数据文件路径
    data = prepare_intraday_data('../../datas/stock-NVDA.csv')
    cerebro.adddata(data)

    # 设置初始资金
    cerebro.broker.setcash(100000.0)

    # 设置佣金 - 万三佣金
    cerebro.broker.setcommission(commission=0.0003)

    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')

    print('初始资金: %.2f' % cerebro.broker.getvalue())

    # 运行回测
    results = cerebro.run()

    # 打印结果
    strat = results[0]
    print('最终资金: %.2f' % cerebro.broker.getvalue())
    print('夏普比率:', strat.analyzers.sharpe.get_analysis()['sharperatio'])
    print('最大回撤:', strat.analyzers.drawdown.get_analysis()['max']['drawdown'])
    print('年化收益率:', strat.analyzers.returns.get_analysis()['rnorm100'])

    # 绘制图表
    cerebro.plot(style='candlestick', volume=True)


if __name__ == '__main__':
    run_backtest()
