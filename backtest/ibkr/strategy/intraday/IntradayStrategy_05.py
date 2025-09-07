import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime, time

from backtrader.indicators import KDJ

import backtrader as bt
import pandas as pd
from datetime import datetime, time

import backtrader as bt
import backtrader.indicators as btind
import pandas as pd
from datetime import datetime, time

class TSI(bt.Indicator):
    """
    真实强度指数(TSI)指标实现
    TSI = 100 * (EMA(EMA(价格变化, 长周期), 短周期) / EMA(EMA(绝对价格变化, 长周期), 短周期))
    """
    lines = ('tsi',)
    params = (
        ('long_period', 25),  # 长周期
        ('short_period', 13),  # 短周期
    )

    def __init__(self):
        # 计算价格变化
        price_change = self.data - self.data(-1)

        # 计算双重EMA平滑的价格变化
        ema1 = btind.EMA(price_change, period=self.p.long_period)
        ema2 = btind.EMA(ema1, period=self.p.short_period)

        # 计算双重EMA平滑的绝对价格变化
        abs_price_change = abs(price_change)
        abs_ema1 = btind.EMA(abs_price_change, period=self.p.long_period)
        abs_ema2 = btind.EMA(abs_ema1, period=self.p.short_period)

        # 计算TSI
        self.lines.tsi = 100 * (ema2 / abs_ema2)


class TSIStrategy(bt.Strategy):
    params = (
        ('ema_period', 20),  # EMA周期
        ('tsi_long_period', 25),  # TSI长周期
        ('tsi_short_period', 13),  # TSI短周期
        ('printlog', False),  # 是否打印日志
    )

    def __init__(self):
        # 创建EMA指标
        self.ema = btind.EMA(self.data.close, period=self.p.ema_period)

        # 创建TSI指标
        self.tsi = TSI(self.data,
                       long_period=self.p.tsi_long_period,
                       short_period=self.p.tsi_short_period)

        # 跟踪订单和交易状态
        self.order = None
        self.buyprice = None
        self.buycomm = None
        # 记录交易
        self.trades = []
        self.position_size = 0
        self.has_position = False



    def next(self):
        # 如果有未完成的订单，不进行任何操作
        if self.order:
            return

        # 检查是否持有头寸
        if not self.has_position:
            # 入场条件：TSI > 0 且收盘价 > EMA
            if self.tsi[0] > 0 and self.data.close[0] > self.ema[0]:
                self.log(f'BUY CREATE, Price: {self.data.close[0]:.2f}, TSI: {self.tsi[0]:.2f}, EMA: {self.ema[0]:.2f}')
                # 买入全部可用资金
                self.order = self.buy(size=1,price=self.data.close[0])
                self.buyprice = self.data.close[0]
                self.has_position = True
        if self.has_position:
            # 退出条件：TSI < 0 且收盘价 < EMA
            if self.tsi[0] < 0 and self.data.close[0] < self.ema[0]:

                # 卖出全部头寸
                if self.data.close[0] - self.buyprice > 2:
                    self.log(
                        f'SELL CREATE, Price: {self.data.close[0]:.2f}, TSI: {self.tsi[0]:.2f}, EMA: {self.ema[0]:.2f}')
                    self.order = self.sell(size=1,price=self.data.close[0])
                    self.has_position = False

    def log_buy_signal(self, signal, price):
        '''记录买入信号'''
        self.log(f'🚀 {signal}, 价格: {price:.2f}')
        self.trade_signals.append({
            'time': self.data1.datetime.datetime(0),
            'type': 'BUY',
            'signal': signal,
            'price': price

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
                    'BUY  EXECUTED, Price: %.2f, size: %.2f,  Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.size,
                     order.executed.value,
                     order.executed.comm))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
                self.high_since_entry = self.data.close[0]  # 设置入场后的最高价
                self.has_position = True
            else:  # Sell
                self.has_position = False
                self.log('SELL EXECUTED, Price: %.2f, size: %.2f,  Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.size,
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
            self.log(f'ema_period: {self.p.ema_period:.2f}')
            self.log(f'tsi_long_period: {self.p.tsi_long_period:.2f}')
            self.log(f'tsi_short_period: {self.p.tsi_short_period:.2f}')
            self.log(f'\n')



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
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')


    print('初始资金: %.2f' % cerebro.broker.getvalue())

    # 参数网格
    param_grid = {
        'ema_period': [10, 15, 20, 25, 30],
        'tsi_long_period': [20, 25, 30, 35, 40],
        'tsi_short_period': [5, 8, 10, 13, 15]
    }

    cerebro.optstrategy(TSIStrategy, **param_grid)

    # 运行优化
    print("开始参数优化...")
    opt_results = cerebro.run(maxcpus=1)

    # 分析结果
    results = []
    for i, run in enumerate(opt_results):
        for strategy in run:
            # 获取分析结果
            sharpe_ratio = strategy.analyzers.sharpe.get_analysis()['sharperatio']
            max_drawdown = strategy.analyzers.drawdown.get_analysis()['max']['drawdown']
            total_return = strategy.analyzers.returns.get_analysis()['rtot']

            # 交易统计
            trade_analysis = strategy.analyzers.trades.get_analysis()
            total_trades = trade_analysis.total.closed if 'total' in trade_analysis else 0
            win_rate = (trade_analysis.won.total / total_trades * 100) if total_trades > 0 else 0

            results.append({
                'ema_period': strategy.p.ema_period,
                'tsi_long_period': strategy.p.tsi_long_period,
                'tsi_short_period': strategy.p.tsi_short_period,
                'final_value': 1,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'total_return_pct': total_return * 100,
                'total_trades': total_trades,
                'win_rate': win_rate
            })


    # 创建结果DataFrame
    results_df = pd.DataFrame(results)

    # 多维度排序
    best_by_sharpe = results_df.sort_values('sharpe_ratio', ascending=False)
    best_by_return = results_df.sort_values('total_return_pct', ascending=False)
    best_by_drawdown = results_df.sort_values('max_drawdown', ascending=True)

    print("\n按夏普比率排序的最佳参数:")
    print(best_by_sharpe.head(5))

    print("\n按总收益排序的最佳参数:")
    print(best_by_return.head(5))

    print("\n按最大回撤排序的最佳参数:")
    print(best_by_drawdown.head(5))

    return results_df

    # # 运行回测
    # results = cerebro.run()
    #
    # # 打印结果
    # strat = results[0]
    # print('最终资金: %.2f' % cerebro.broker.getvalue())
    # print('夏普比率:', strat.analyzers.sharpe.get_analysis()['sharperatio'])
    # print('最大回撤:', strat.analyzers.drawdown.get_analysis()['max']['drawdown'])
    # print('年化收益率:', strat.analyzers.returns.get_analysis()['rnorm100'])
    #
    # # 绘制图表
    # cerebro.plot(style='candlestick', volume=True)


if __name__ == '__main__':
    results_df = run_backtest()

    # 保存结果到CSV
    results_df.to_csv('parameter_optimization_results.csv', index=False)
    print("结果已保存到 parameter_optimization_results.csv")
