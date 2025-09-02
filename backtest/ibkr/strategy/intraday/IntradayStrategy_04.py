import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime, time

from backtrader.indicators import KDJ

import backtrader as bt
import pandas as pd
from datetime import datetime, time


class MultiTimeframeRSI_KDJ_Strategy(bt.Strategy):
    params = (
        # RSI参数
        ('rsi_period', 14),
        ('rsi_oversold', 30),
        ('rsi_overbought', 70),
        # KDJ参数
        ('kdj_period', 9),
        ('kdj_period_d', 3),
        ('kdj_oversold', 20),
        ('kdj_overbought', 70),
        # 交易参数
        ('stop_loss', 0.015),
        ('take_profit', 0.03),
        ('position_size', 0.8),
        ('trail_stop', 0.01),
        # 多周期参数
        ('minute1_compression', 1),  # 1分钟线
        ('minute5_compression', 5),  # 5分钟线
    )

    def __init__(self):
        self.order = None
        self.buyprice = None
        self.entry_price = 0
        self.high_since_entry = 0
        self.trade_signals = []

        # 获取不同时间框架的数据
        self.data1 = self.datas[0]  # 1分钟数据（主数据）
        self.data5 = self.datas[1]  # 5分钟数据

        # 1分钟指标
        self.rsi1 = bt.ind.RSI(self.data.close, period=self.params.rsi_period)
        self.kdj1 = KDJ(self.data1, period=self.params.kdj_period, d_period=self.params.kdj_period_d)
        self.ema_fast1 = bt.ind.EMA(self.data.close, period=5)
        self.ema_slow1 = bt.ind.EMA(self.data.close, period=20)

        # 5分钟指标 - 需要重新计算，不能直接用data1的指标
        self.rsi5 = bt.ind.RSI(self.data5.close, period=self.params.rsi_period)
        self.kdj5 = KDJ(self.data5)
        self.ema_fast5 = bt.ind.EMA(self.data5.close, period=5)
        self.ema_slow5 = bt.ind.EMA(self.data5.close, period=20)


        # 成交量确认
        self.volume_sma1 = bt.ind.SMA(self.data1.volume, period=20)
        self.volume_sma5 = bt.ind.SMA(self.data5.volume, period=20)

        # 记录交易
        self.trades = []
        self.position_size = 0
        self.has_position = False

    def next(self):
        if self.order:
            return

        current_price = self.data1.close[0]
        current_time = self.data1.datetime.time()



        if not self.has_position:
            self.check_buy_signals(current_price)
        else:
            self.check_sell_signals(current_price)

    def check_buy_signals(self, current_price):
        '''检查多周期共振买入信号'''
        buy_signals = []

        # 1. RSI多周期共振
        rsi1_oversold = self.rsi1[0] < self.params.rsi_oversold
        rsi5_oversold = self.rsi5[0] < self.params.rsi_oversold if not np.isnan(self.rsi5[0]) else False

        if rsi1_oversold or rsi5_oversold:
            buy_signals.append('RSI双周期超卖共振')

        # 2. KDJ多周期共振
        kdj1_oversold = self.kdj1.k[0] < self.params.kdj_oversold
        kdj5_oversold = self.kdj5.k[0] < self.params.kdj_oversold if not np.isnan(self.kdj5.k[0]) else False

        if kdj1_oversold or kdj5_oversold:
            buy_signals.append('KDJ双周期++超卖共振')

        # 3. KDJ金叉共振
        kdj1_golden_cross = (self.kdj1.k[0] > self.kdj1.d[0] and self.kdj1.k[-1] <= self.kdj1.d[-1])
        kdj5_golden_cross = (self.kdj5.k[0] > self.kdj5.d[0] and self.kdj5.k[-1] <= self.kdj5.d[-1] if not np.isnan(
            self.kdj5.k[0]) else False)

        if kdj1_golden_cross or kdj5_golden_cross:
            buy_signals.append('KDJ双周期+__金叉共振')

        kdj5_j_sign = self.kdj5.k[0] > self.kdj5.d[-1]   if not np.isnan(self.kdj5.j[0]) else False
        if kdj5_j_sign:
            buy_signals.append('KDJ双周期 kdj5_j_sign')

        # 4. 趋势确认 - 双周期均线多头排列
        trend_confirm1 = (current_price > self.ema_fast1[0] > self.ema_slow1[0])
        trend_confirm5 = (self.data5.close[0] > self.ema_fast5[0] > self.ema_slow5[0])

        if not (trend_confirm1 and trend_confirm5):
            return  # 趋势不共振，不交易

        # 5. 成交量确认
        volume_confirm1 = self.data1.volume[0] > self.volume_sma1[0]
        volume_confirm5 = self.data5.volume[0] > self.volume_sma5[0] if not np.isnan(self.volume_sma5[0]) else False

        # 多周期共振买入条件
        if ( trend_confirm1 and trend_confirm5 and
                volume_confirm1 and volume_confirm5):

            # 确保所有信号都有效
            valid_signals = [sig for sig in buy_signals if sig]
            if len(valid_signals) >= 2:  # 至少2个共振信号
                size = self.broker.getcash() * self.params.position_size / current_price
                self.position_size  = int(size / 100) * 100

                signal_info = f"多周期共振买入: {', '.join(valid_signals)}"
                self.log_buy_signal(signal_info, current_price)
                self.order = self.buy(size=self.position_size, price=current_price)
                self.entry_price = current_price
                self.high_since_entry = current_price


    def check_sell_signals(self, current_price):
        '''检查多周期共振卖出信号'''
        if current_price > self.high_since_entry:
            self.high_since_entry = current_price

        sell_signals = []

        # 1. RSI多周期超买
        rsi1_overbought = self.rsi1[0] > self.params.rsi_overbought
        rsi5_overbought = self.rsi5[0] > self.params.rsi_overbought if not np.isnan(self.rsi5[0]) else False

        if rsi1_overbought or rsi5_overbought:
            sell_signals.append('RSI超买')

        # 2. KDJ多周期超买
        kdj1_overbought = self.kdj1.k[0] > self.params.kdj_overbought
        kdj5_overbought = self.kdj5.k[0] > self.params.kdj_overbought if not np.isnan(
            self.kdj5.k[0]) else False

        if kdj1_overbought or kdj5_overbought:
            sell_signals.append('KDJ超买')

        # 3. KDJ死叉
        kdj1_dead_cross = self.kdj1.k[0] < self.kdj1.d[0] and self.kdj1.k[-1] >= self.kdj1.d[-1]
        kdj5_dead_cross = (self.kdj5.k[0] < self.kdj5.d[0] and
                           self.kdj5.k[-1] >= self.kdj5.d[-1] if not np.isnan(self.kdj5.k[0]) else False)

        if kdj1_dead_cross or kdj5_dead_cross:
            sell_signals.append('KDJ死叉')

        # 4. 风险管理
        stop_loss_price = self.entry_price * (1 - self.params.stop_loss)
        if current_price <= stop_loss_price:
            sell_signals.append(f'止损({self.params.stop_loss * 100}%)')

        take_profit_price = self.entry_price * (1 + self.params.take_profit)
        if current_price >= take_profit_price:
            sell_signals.append(f'止盈({self.params.take_profit * 100}%)')

        trail_stop_price = self.high_since_entry * (1 - self.params.trail_stop)
        if current_price <= trail_stop_price:
            sell_signals.append(f'追踪止损({self.params.trail_stop * 100}%)')

        # 卖出条件相对宽松，任何一个信号触发即可
        if sell_signals:
            signal_info = f"卖出信号: {', '.join(sell_signals)}"
            self.log_sell_signal(signal_info, current_price)
            self.order = self.sell(size=self.position_size, price=current_price)
            self.buy_bracket()

    def log_buy_signal(self, signal, price):
        '''记录买入信号'''
        self.log(f'🚀 {signal}, 价格: {price:.2f}')
        self.trade_signals.append({
            'time': self.data1.datetime.datetime(0),
            'type': 'BUY',
            'signal': signal,
            'price': price,
            'rsi1': self.rsi1[0],
            'rsi5': self.rsi5[0],
            'kdj_k1': self.kdj1.k[0],
            'kdj_d1': self.kdj1.d[0],
            'kdj_k5': self.kdj5.k[0],
            'kdj_d5': self.kdj5.d[0]
        })

    def log_sell_signal(self, signal, price):
        '''记录卖出信号'''
        profit_pct = (price / self.entry_price - 1) * 100
        self.log(f'📉 {signal}, 价格: {price:.2f}, 收益: {profit_pct:.2f}%')
        self.trade_signals.append({
            'time': self.data1.datetime.datetime(0),
            'type': 'SELL',
            'signal': signal,
            'price': price,
            'profit_pct': profit_pct
        })

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

    def log(self, txt, dt=None):
        ''' 日志函数 '''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

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
    cerebro.addstrategy(MultiTimeframeRSI_KDJ_Strategy)

    # 加载数据 - 请替换为您的数据文件路径
    data = prepare_intraday_data('../../datas/stock-NVDA.csv')
    cerebro.adddata(data)
    cerebro.resampledata(data, timeframe=bt.TimeFrame.Minutes, compression=5)  # 5分钟

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
