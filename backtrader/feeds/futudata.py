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

import datetime

from futu import *

from backtrader import TimeFrame, date2num
from backtrader.feed import DataBase
from backtrader.stores import futustore
from backtrader.utils.py3 import (with_metaclass)


class MetaFutuData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaFutuData, cls).__init__(name, bases, dct)

        # Register with the store
        futustore.FutuStore.DataCls = cls


class FutuData(with_metaclass(MetaFutuData, DataBase)):
    '''futu  Brokers Data Feed.


    '''
    params = (
        ('symbol', list()),  # 股票代码
        ('trading_period', KLType.K_1M),  # 时间框架
        ('start_date', None),  # 开始日期
        ('end_date', None),  # 结束日期
    )

    _store = futustore.FutuStore

    # Minimum size supported by real-time bars
    RTBAR_MINSIZE = (TimeFrame.Seconds, 5)

    # States for the Finite State Machine in _load
    _ST_FROM, _ST_START, _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(5)

    def _timeoffset(self):
        return self.futu.timeoffset()

    def islive(self):
        '''Returns ``True`` to notify ``Cerebro`` that preloading and runonce
        should be deactivated'''
        return True

    def __init__(self, **kwargs):
        self._started = None
        self.futu = self._store(**kwargs)
        self.quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

    def islive(self):
        '''Returns ``True`` to notify ``Cerebro`` that preloading and runonce
        should be deactivated'''
        return True

    def setenvironment(self, env):
        '''Receives an environment (cerebro) and passes it over to the store it
        belongs to'''
        super(FutuData, self).setenvironment(env)
        env.addstore(self.futu)

    def start(self):
        '''Starts the IB connecction and gets the real contract and
        contractdetails if it exists'''
        super(FutuData, self).start()
        if self.p.start_date is None:
            self.p.start_date = datetime.datetime(2022, 1, 1)
        if self.p.end_date is None:
            self.p.end_date = datetime.datetime.now()
        self._started = True

    def stop(self):
        '''Stops and tells the store to stop'''
        super(FutuData, self).stop()
        self.quote_ctx.close()

    def _load(self):
        self.quote_ctx.set_handler(OnTickClass())

        ret, data = self.quote_ctx.subscribe(code_list=self.p.symbol,
                                             subtype_list=[SubType.TICKER, self.p.trading_period])
        if ret == RET_OK:
            if data is not None:
                print(data)
                self.data = data
        else:
            print('error:', data)

    def _get_next(self):
        if len(self.data) == 0:
            return False

        row = self.data.iloc[0]
        self.lines.datetime[0] = date2num(pd.to_datetime(row['time_key']))
        self.lines.open[0] = row['open']
        self.lines.high[0] = row['high']
        self.lines.low[0] = row['low']
        self.lines.close[0] = row['close']
        self.lines.volume[0] = row['volume']

        self.data = self.data.iloc[1:]
        return True


class OnTickClass(TickerHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret_code, data = super(OnTickClass, self).on_recv_rsp(rsp_pb)
        if ret_code != RET_OK:
            print("TickerTest: error, msg: %s" % data)
            return RET_ERROR, data
        print("TickerTest ", data)  # TickerTest 自己的处理逻辑
        return RET_OK, data
