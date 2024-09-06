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
        self.dataclose = self.datas[0].close

    def next(self):
        print(f'Close: {self.dataclose[0]}')


ib_symbol = 'EUR.USD-CASH-IDEALPRO'
compression = 5


def run(args=None):
    cerebro = bt.Cerebro()

    # 使用自定义数据源
    data = FutuData(symbol='AAPL', start_date='2023-01-01', end_date='2023-12-31')
    cerebro.adddata(data)

    cerebro.addstrategy(TestStrategy)
    cerebro.run()


if __name__ == '__main__':
    run()
