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
from pandas._libs.tslibs.timezones import dateutil_gettz as gettz

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


DATA_COLUMNS = ('code',
                'open', 'close', ' high', 'low', 'volume', 'turnover', 'pe_ratio', 'turnover_rate',
                'last_close')


class FutuData(with_metaclass(MetaFutuData, DataBase)):
    """futu  Brokers Data Feed.


    """
    params = (
        ('symbol', ''),  # 股票代码
        ('trading_period', KLType.K_3M),  # 时间框架
        ('fromdate', None),  # 开始日期
        ('todate', None),  # 结束日期
        ('subscribeDataType', 'quote'),  # 结束日期
        ('precision', 0.00001),  # 结束日期
    )

    lines = DATA_COLUMNS
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
        self._tzinput = False
        self._tz = datetime.timezone("US/Eastern")
        self.code = self.p.symbol
        self._calendar = None

    def islive(self):
        '''Returns ``True`` to notify ``Cerebro`` that preloading and runonce
        should be deactivated'''
        return True

    def haslivedata(self):
        return True

    def setenvironment(self, env):
        '''Receives an environment (cerebro) and passes it over to the store it
        belongs to'''
        super(FutuData, self).setenvironment(env)
        env.addstore(self.futu)

    def start(self):
        super(FutuData, self).start()
        '''Starts the futu connecction '''
        if self.p.fromdate is None:
            self.fromdate = date2num(datetime.fromtimestamp(time.time()))
        else:
            self.fromdate = date2num(dt.datetime.strptime(self.p.fromdate, '%Y-%m-%d'))

        if self.p.todate is None:
            self.todate = date2num(datetime.fromtimestamp(time.time()))
        else:
            self.todate = date2num(dt.datetime.strptime(self.p.todate, '%Y-%m-%d'))
        self._started = True
        self.tz = "US/Eastern"

    def stop(self):
        '''Stops and tells the store to stop'''
        super(FutuData, self).stop()
        print("quote_ctx.close()")
        self.quote_ctx.close()

    def _load(self):
        self.quote_ctx.set_handler(OnTickClass())

        if self.p.subscribeDataType is None:
            ret, data = self.quote_ctx.subscribe(code_list=self.p.symbol, subtype_list=[SubType.K_5M])
            if ret == RET_OK:
                ret, data = self.quote_ctx.get_cur_kline(self.p.symbol, 1, KLType.K_5M, AuType.QFQ)
                if ret == RET_OK:
                    if data is not None:
                        self.lines.open[0] = data['open'][0]
                        self.lines.close[0] = data['close'][0]
                        self.lines.high[0] = data['high'][0]
                        self.lines.low[0] = data['low'][0]
                        self.lines.volume[0] = data['volume'][0]
                        self.lines.turnover[0] = data['turnover'][0]
                        self.lines.last_close[0] = data['last_close'][0]
                        local_time = dt.datetime.strptime(data['time_key'][0], '%Y-%m-%d %H:%M:%S')
                        self.lines.datetime[0] = date2num(local_time)
                        self.current_data = local_time
                        if self.previous_data == self.current_data:
                            return False  # 重复数据，跳过
                            # 更新上一次的数据
                        self.previous_data = self.current_data
                        return True  # 数据有效
                else:
                    print('error:', data)
            else:
                print('error:', data)
        else:
            ret_sub, err_message = self.quote_ctx.subscribe([self.p.symbol], [SubType.QUOTE], subscribe_push=True)
            # 先订阅 K 线类型。订阅成功后 OpenD 将持续收到服务器的推送，False 代表暂时不需要推送给脚本
            if ret_sub == RET_OK:  # 订阅成功
                ret, data = self.quote_ctx.get_stock_quote([self.p.symbol])  # 获取订阅股票报价的实时数据
                if ret == RET_OK:
                    # print(data)
                    self.lines.close[0] = data['last_price'][0]
                    self.lines.open[0] = data['open_price'][0]
                    self.lines.high[0] = data['high_price'][0]
                    self.lines.low[0] = data['low_price'][0]
                    self.lines.volume[0] = data['volume'][0]
                    self.lines.turnover[0] = data['turnover'][0]
                    data_date = data['data_date'][0]
                    data_time = data['data_time'][0]
                    local_time = dt.datetime.strptime(f"""{data_date} {data_time}""", '%Y-%m-%d %H:%M:%S')
                    self.lines.datetime[0] = date2num(local_time)
                    return True
                else:
                    print('error:', data)
            else:
                print('subscription failed', err_message)


class OnTickClass(TickerHandlerBase):
    def on_recv_rsp(self, rsp_pb):
        ret_code, data = super(OnTickClass, self).on_recv_rsp(rsp_pb)
        if ret_code != RET_OK:
            print("TickerTest: error, msg: %s" % data)
            return RET_ERROR, data
        print("TickerTest ", data)  # TickerTest 自己的处理逻辑
        return RET_OK, data
