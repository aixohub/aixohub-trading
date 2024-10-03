import datetime
import os
import sys
import strategy_rsi
import pandas as pd
from btplotting import BacktraderPlottingLive, BacktraderPlotting

import backtrader as bt

date_formate_1 = "%Y-%m-%d %H:%M:%S"
date_formate_2 = "%Y%m%d"


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


def run_backtest(param):
    cerebro = bt.Cerebro()
    # 加载数据
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    modpath = os.path.dirname(modpath)
    datapath = os.path.join(modpath, param['data'])
    datas = pd.read_csv(datapath)

    datas['datetime'] = pd.to_datetime(datas['datetime'], format=param['date_formate'])
    datas = datas[['code', 'datetime', 'open', 'high', 'low', 'close', 'volume']].sort_values(by=['datetime'])

    datafeed = My_PandasData(dataname=datas)
    cerebro.adddata(datafeed, name='nvda')

    cerebro.broker.setcash(10000.0)

    # 加载策略
    cerebro.addstrategy(strategy=strategy_rsi.RsiStrategy)

    # 加载
    cerebro.addanalyzer(bt.analyzers.Returns, _name='收益')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer)
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='AnnualReturn')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.0, annualize=True, _name='SharpeRatio')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown')
    cerebro.addanalyzer(BacktraderPlottingLive, address='127.0.0.1', port=8899)

    # 开始回测
    cerebro.run()
    # p = BacktraderPlotting(style='bar', scheme=Tradimo())
    # p = BacktraderPlotting(style='bar', multiple_tabs=True)
    p = BacktraderPlotting(style='bar')
    cerebro.plot(p)


if __name__ == '__main__':
    param = {
        "data": "datas/516510.SH.csv",
        "date_formate": date_formate_2
    }
    para2 = {
        "data": "datas/stock-NVDA.csv",
        "date_formate": date_formate_1
    }
    run_backtest(para2)
