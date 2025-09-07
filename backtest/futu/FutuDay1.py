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

from futu import *

import backtrader as bt
from backtrader.brokers.futubroker import FutuBroker
from backtrader.feeds import IBData

import pandas as pd
pd.options.display.max_rows=5000
pd.options.display.max_columns=5000
pd.options.display.width=1000


api_host = '127.0.0.1'
api_port = 4002
code ='tsla'


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


        print('--------------------------------------------------')
        print('TestStrategy Created')
        print('--------------------------------------------------')

    def next(self):
        print(
            f''' code: {self.data.code} datetime: {self.datas[0].datetime.datetime(0)} volume: {self.data.volume[0]}  Close: {self.data.close[0]}  ''')




def run(args=None):
    cerebro = bt.Cerebro()
    # code = get_option_chain('US.NVDA')
    # code = 'HK.03690'
    # 使用自定义数据源
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
                  initAccountFlag=False,
                  timeframe=bt.TimeFrame.Minutes
                  )
    # cerebro.resampledata(data, timeframe=bt.TimeFrame.Minutes, compression=1)
    cerebro.adddata(data)
    broker = FutuBroker()
    cerebro.setbroker(broker)


    cerebro.addstrategy(TestStrategy)
    cerebro.run()

def get_option_chain(code):
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    ret1, data1 = quote_ctx.get_option_expiration_date(code=code)

    filter1 = OptionDataFilter()
    filter1.delta_min = 0
    filter1.delta_max = 0.1

    option_code = ''
    if ret1 == RET_OK:
        expiration_date_list = data1['strike_time'].values.tolist()
        index = 1
        for date in expiration_date_list:
            ret2, data2 = quote_ctx.get_option_chain(code=code, start=date, end=date, data_filter=filter1)
            if ret2 == RET_OK:
                # print(data2)
                if index == 2:
                    option_code = data2['code'][39]
                    print(f"""option_code : {option_code}""")
                    quote_ctx.close()
                    return "US.NVDA240913C110000"
            else:
                print('error:', data2)
            index = index + 1
            time.sleep(3)
        else:
            print('error:', data1)
    return option_code



if __name__ == '__main__':
   # run()

   broker = FutuBroker()
   broker.start()
   broker.buy(size=1, price=1, symbol='US.NVDA250912C190000')
   broker.stop()
