#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015-2023 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################


from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import time

import backtrader as bt
from backtest.ibkr.logger_conf import setup_logging
from backtrader.feeds import IBData


class TestStrategy(bt.Strategy):
    params = dict(
        smaperiod=3,
        short_period=5,
        long_period=30,
        stop_loss=0.02,
        period=16,
        long_ravg=25, short_ravg=12,
        max_position=10, spike_window=4,
        cls=0.5, csr=-0.1, clr=-0.3,
    )

    def __init__(self):
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
        print(
            f''' code: {self.data.code}''')
        # signal = self.p.cls * self.ls_cross + self.p.clr * self.lr_cross + self.p.csr * self.sr_cross
        #
        # print(
        #     f''' code: {self.data.code} datetime: {self.datas[0].datetime.datetime(0)} rsi: {signal} turnover: {self.data.turnover[0]} volume: {self.data.volume[0]}  Close: {self.data.close[0]}  ''')


def run(args=None):
    cerebro = bt.Cerebro()
    code = 'NVDA'
    # 使用自定义数据源
    data = IBData(host='127.0.0.1', port=4001, clientId=12,
                  name=code,
                  dataname=code,
                  secType='STK',
                  what='BID_ASK',
                  exchange="SMART",
                  currency='USD'
                  )
    cerebro.adddata(data)

    cerebro.addstrategy(TestStrategy)
    cerebro.run()


if __name__ == '__main__':
    setup_logging()
    run()
