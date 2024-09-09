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

import backtrader as bt
from backtrader.feeds import FutuData


class TestStrategy(bt.Strategy):
    def __init__(self):
        self.open = self.datas[0].open
        self.close = self.datas[0].close
        self.high = self.datas[0].high
        self.low = self.datas[0].low
        self.volume = self.datas[0].volume
        self.turnover = self.datas[0].turnover

    def next(self):
        # aa = self.datas[0].datetime.datetime(0)
        print(f''' turnover: {self.turnover[0]}  open: {self.open[0]}  Close: {self.close[0]}  high: {self.high[0]}   low: {self.low[0]}   volume: {self.volume[0]}  ''')


ib_symbol = 'EUR.USD-CASH-IDEALPRO'
compression = 5


def run(args=None):
    cerebro = bt.Cerebro()
    # code = get_option_chain('US.NVDA')
    code = 'HK.03690'
    # 使用自定义数据源
    data = FutuData(symbol=code, fromdate='2024-09-01', todate='2024-09-30')
    cerebro.adddata(data)

    cerebro.addstrategy(TestStrategy)
    cerebro.run()


from futu import *
import time


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
                    return "US.NVDA240913P95000"
            else:
                print('error:', data2)
            index = index + 1
            time.sleep(3)
        else:
            print('error:', data1)
    return option_code


if __name__ == '__main__':
    run()
