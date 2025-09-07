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

import collections
from copy import copy
from datetime import date, datetime, timedelta
import threading

from futu import *

from backtrader.feed import DataBase
from backtrader import (TimeFrame, num2date, date2num, BrokerBase,
                        Order, BuyOrder, SellOrder, OrderBase, OrderData)
from backtrader.utils.py3 import bytes, with_metaclass, MAXFLOAT
from backtrader.metabase import MetaParams
from backtrader.comminfo import CommInfoBase
from backtrader.position import Position
from backtrader.stores import futustore
from backtrader.utils import AutoDict, AutoOrderedDict
from backtrader.comminfo import CommInfoBase


class futuOrder(OrderBase):
    '''Subclasses the IBPy order to provide the minimum extra functionality
    needed to be compatible with the internally defined orders

    Once ``OrderBase`` has processed the parameters, the __init__ method takes
    over to use the parameter values and set the appropriate values in the
    ib.ext.Order.Order object

    Any extra parameters supplied with kwargs are applied directly to the
    ib.ext.Order.Order object, which could be used as follows::

      Example: if the 4 order execution types directly supported by
      ``backtrader`` are not enough, in the case of for example
      *Interactive Brokers* the following could be passed as *kwargs*::

        orderType='LIT', lmtPrice=10.0, auxPrice=9.8

      This would override the settings created by ``backtrader`` and
      generate a ``LIMIT IF TOUCHED`` order with a *touched* price of 9.8
      and a *limit* price of 10.0.

    This would be done almost always from the ``Buy`` and ``Sell`` methods of
    the ``Strategy`` subclass being used in ``Cerebro``
    '''

    def __str__(self):
        '''Get the printout from the base class and add some ib.Order specific
        fields'''
        basetxt = super(futuOrder, self).__str__()
        tojoin = [basetxt]
        tojoin.append('Ref: {}'.format(self.ref))
        tojoin.append('Ref: {}'.format(self.ref))
        tojoin.append('orderId: {}'.format(self.orderId))
        tojoin.append('trd_side: {}'.format(self.trd_side))
        tojoin.append('Size (ib): {}'.format(self.totalQuantity))
        tojoin.append('Lmt Price: {}'.format(self.lmtPrice))
        tojoin.append('Aux Price: {}'.format(self.auxPrice))
        tojoin.append('OrderType: {}'.format(self.orderType))
        tojoin.append('Tif (Time in Force): {}'.format(self.tif))
        tojoin.append('GoodTillDate: {}'.format(self.goodTillDate))
        return '\n'.join(tojoin)

    # Map backtrader order types to the ib specifics
    _IBOrdTypes = {
        None: bytes('MKT'),  # default
        Order.Market: bytes('MKT'),
        Order.Limit: bytes('LMT'),
        Order.Close: bytes('MOC'),
        Order.Stop: bytes('STP'),
        Order.StopLimit: bytes('STPLMT'),
        Order.StopTrail: bytes('TRAIL'),
        Order.StopTrailLimit: bytes('TRAIL LIMIT'),
    }

    def __init__(self, action, **kwargs):

        # Marker to indicate an openOrder has been seen with
        # PendinCancel/Cancelled which is indication of an upcoming
        # cancellation
        self._willexpire = False

        self.ordtype = self.Buy if action == 'BUY' else self.Sell

        super(futuOrder, self).__init__()

        # Now fill in the specific IB parameters
        self.orderType = self._IBOrdTypes[self.exectype]
        self.permid = 0

        # 'B' or 'S' should be enough
        self.trd_side = action

        # Set the prices
        self.lmtPrice = 0.0
        self.auxPrice = 0.0

        if self.exectype == self.Market:  # is it really needed for Market?
            pass
        elif self.exectype == self.Close:  # is it ireally needed for Close?
            pass
        elif self.exectype == self.Limit:
            self.lmtPrice = self.price
        elif self.exectype == self.Stop:
            self.auxPrice = self.price  # stop price / exec is market
        elif self.exectype == self.StopLimit:
            self.lmtPrice = self.pricelimit  # req limit execution
            self.auxPrice = self.price  # trigger price
        elif self.exectype == self.StopTrail:
            if self.trailamount is not None:
                self.auxPrice = self.trailamount
            elif self.trailpercent is not None:
                # value expected in % format ... multiply 100.0
                self.m_trailingPercent = self.trailpercent * 100.0
        elif self.exectype == self.StopTrailLimit:
            self.m_trailStopPrice = self.lmtPrice = self.price
            # The limit offset is set relative to the price difference in TWS
            self.lmtPrice = self.pricelimit
            if self.trailamount is not None:
                self.auxPrice = self.trailamount
            elif self.trailpercent is not None:
                # value expected in % format ... multiply 100.0
                self.m_trailingPercent = self.trailpercent * 100.0

        self.totalQuantity = abs(self.size)  # ib takes only positives

        self.size = abs(self.size)

        # self.m_transmit = self.transmit
        if self.parent is not None:
            self.m_parentId = self.parent.orderId

        # Time In Force: DAY, GTC
        if self.valid is None:
            tif = 'GTC'  # Good til cancelled
        elif isinstance(self.valid, (datetime, date)):
            tif = 'GTD'  # Good til date
            self.goodTillDate = bytes(self.valid.strftime('%Y%m%d %H:%M:%S'))
        elif isinstance(self.valid, (timedelta,)):
            if self.valid == self.DAY:
                tif = 'DAY'
            else:
                tif = 'GTD'  # Good til date
                valid = datetime.now() + self.valid  # .now, using localtime
                self.goodTillDate = bytes(valid.strftime('%Y%m%d %H:%M:%S'))

        elif self.valid == 0:
            tif = 'DAY'
        else:
            tif = 'GTD'  # Good til date
            valid = num2date(self.valid)
            self.goodTillDate = bytes(valid.strftime('%Y%m%d %H:%M:%S'))

        self.tif = tif
        self.time_in_force = TimeInForce.GTC

        # OCA
        self.ocaType = 1  # Cancel all remaining orders with block

        # pass any custom arguments to the order
        for key, value in kwargs.items():
            setattr(self, key, value)


class FutuCommInfo(CommInfoBase):
    def getvaluesize(self, size, price):
        # In real life the margin approaches the price
        return abs(size) * price

    def getoperationcost(self, size, price):
        '''Returns the needed amount of cash an operation would cost'''
        # Same reasoning as above
        return abs(size) * price


class MetaFutuBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaFutuBroker, cls).__init__(name, bases, dct)
        futustore.FutuStore.BrokerCls = cls


class FutuBroker(with_metaclass(MetaFutuBroker, BrokerBase)):
    '''Broker implementation for Oanda.

    This class maps the orders/positions from Oanda to the
    internal API of ``backtrader``.

    Params:

      - ``use_positions`` (default:``True``): When connecting to the broker
        provider use the existing positions to kickstart the broker.

        Set to ``False`` during instantiation to disregard any existing
        position
    '''
    params = (
        ('use_positions', True),

        ('trd_pwd', "111111"),
        ('trd_env', TrdEnv.REAL),
        ('trdMarket', TrdMarket.US),
        ('host', '127.0.0.1'),
        ('port', 11111),
        ('security_firm', SecurityFirm.FUTUSECURITIES),

        ('commission', FutuCommInfo(mult=1.0, stocklike=False)),
    )

    def __init__(self, **kwargs):
        super(FutuBroker, self).__init__()

        self.futu = futustore.FutuStore(**kwargs)

        self.orders = collections.OrderedDict()  # orders by order id
        self.notifs = collections.deque()  # holds orders which are notified

        self.opending = collections.defaultdict(list)  # pending transmission
        self.brackets = dict()  # confirmed brackets

        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0
        self.positions = collections.defaultdict(Position)

        self.trd_ctx = OpenSecTradeContext(filter_trdmarket=self.p.trdMarket, host=self.p.host, port=self.p.port,
                                           security_firm=self.p.security_firm)

    def start(self):
        super(FutuBroker, self).start()
        ret, data = self.trd_ctx.unlock_trade(self.p.trd_pwd)
        if ret == RET_OK:
            print("unlock success")

        self.get_account_list()
        self.futu.start(broker=self)
        self.startingcash = self.cash = cash = self.futu.get_cash()
        self.startingvalue = self.value = self.futu.get_value()

        if self.p.use_positions:
            self.positions = self.futu.get_positions()
            for k, v in self.positions.items():
                print(k, v)

    def data_started(self, data):
        pos = self.getposition(data)

        if pos.size < 0:
            order = SellOrder(data=data,
                              size=pos.size, price=pos.price,
                              exectype=Order.Market,
                              simulated=True)

            order.addcomminfo(self.getcommissioninfo(data))
            order.execute(0, pos.size, pos.price,
                          0, 0.0, 0.0,
                          pos.size, 0.0, 0.0,
                          0.0, 0.0,
                          pos.size, pos.price)

            order.completed()
            self.notify(order)

        elif pos.size > 0:
            order = BuyOrder(data=data,
                             size=pos.size, price=pos.price,
                             exectype=Order.Market,
                             simulated=True)

            order.addcomminfo(self.getcommissioninfo(data))
            order.execute(0, pos.size, pos.price,
                          0, 0.0, 0.0,
                          pos.size, 0.0, 0.0,
                          0.0, 0.0,
                          pos.size, pos.price)

            order.completed()
            self.notify(order)

    def stop(self):
        super(FutuBroker, self).stop()
        self.trd_ctx.close()
        self.futu.stop()

    def getcash(self):
        # This call cannot block if no answer is available from oanda
        self.cash = cash = self.o.get_cash()
        return cash

    def getvalue(self, datas=None):
        self.value = self.o.get_value()
        return self.value

    def getposition(self, data, clone=True):
        # return self.o.getposition(data._dataname, clone=clone)
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()

        return pos

    def orderstatus(self, order):
        o = self.orders[order.ref]
        return o.status

    def _submit(self, oref):
        order = self.orders[oref]
        order.submit(self)
        self.notify(order)
        for o in self._bracketnotif(order):
            o.submit(self)
            self.notify(o)

    def _reject(self, oref):
        order = self.orders[oref]
        order.reject(self)
        self.notify(order)
        self._bracketize(order, cancel=True)

    def _accept(self, oref):
        order = self.orders[oref]
        order.accept()
        self.notify(order)
        for o in self._bracketnotif(order):
            o.accept(self)
            self.notify(o)

    def _cancel(self, oref):
        order = self.orders[oref]
        order.cancel()
        self.notify(order)
        self._bracketize(order, cancel=True)

    def _expire(self, oref):
        order = self.orders[oref]
        order.expire()
        self.notify(order)
        self._bracketize(order, cancel=True)

    def _bracketnotif(self, order):
        pref = getattr(order.parent, 'ref', order.ref)  # parent ref or self
        br = self.brackets.get(pref, None)  # to avoid recursion
        return br[-2:] if br is not None else []

    def _bracketize(self, order, cancel=False):
        pref = getattr(order.parent, 'ref', order.ref)  # parent ref or self
        br = self.brackets.pop(pref, None)  # to avoid recursion
        if br is None:
            return

        if not cancel:
            if len(br) == 3:  # all 3 orders in place, parent was filled
                br = br[1:]  # discard index 0, parent
                for o in br:
                    o.activate()  # simulate activate for children
                self.brackets[pref] = br  # not done - reinsert children

            elif len(br) == 2:  # filling a children
                oidx = br.index(order)  # find index to filled (0 or 1)
                self._cancel(br[1 - oidx].ref)  # cancel remaining (1 - 0 -> 1)
        else:
            # Any cancellation cancel the others
            for o in br:
                if o.alive():
                    self._cancel(o.ref)

    def _fill(self, oref, size, price, ttype, **kwargs):
        order = self.orders[oref]

        if not order.alive():  # can be a bracket
            pref = getattr(order.parent, 'ref', order.ref)
            if pref not in self.brackets:
                msg = ('Order fill received for {}, with price {} and size {} '
                       'but order is no longer alive and is not a bracket. '
                       'Unknown situation')
                msg.format(order.ref, price, size)
                self.put_notification(msg, order, price, size)
                return

            # [main, stopside, takeside], neg idx to array are -3, -2, -1
            if ttype == 'STOP_LOSS_FILLED':
                order = self.brackets[pref][-2]
            elif ttype == 'TAKE_PROFIT_FILLED':
                order = self.brackets[pref][-1]
            else:
                msg = ('Order fill received for {}, with price {} and size {} '
                       'but order is no longer alive and is a bracket. '
                       'Unknown situation')
                msg.format(order.ref, price, size)
                self.put_notification(msg, order, price, size)
                return

        data = order.data
        pos = self.getposition(data, clone=False)
        psize, pprice, opened, closed = pos.update(size, price)

        comminfo = self.getcommissioninfo(data)

        closedvalue = closedcomm = 0.0
        openedvalue = openedcomm = 0.0
        margin = pnl = 0.0

        order.execute(data.datetime[0], size, price,
                      closed, closedvalue, closedcomm,
                      opened, openedvalue, openedcomm,
                      margin, pnl,
                      psize, pprice)

        if order.executed.remsize:
            order.partial()
            self.notify(order)
        else:
            order.completed()
            self.notify(order)
            self._bracketize(order)

    def get_account_list(self):
        ret, data = self.trd_ctx.get_acc_list()
        if ret == RET_OK:
            print(data)
            print(data['acc_id'][0])  # 取第一个账号
            print(data['acc_id'].values.tolist())  # 转为 list
        else:
            print('get_acc_list error: ', data)

    def submit(self, order):
        order.submit(self)

        ret, data = self.trd_ctx.place_order(price=order.price, qty=order.size, code=order.symbol,
                                             trd_side=order.trd_side, time_in_force=order.time_in_force,
                                             fill_outside_rth=True, trd_env=self.p.trd_env, remark="my test")
        if ret == RET_OK:
            print(data)
            order.orderId = data['order_id'][0]
            self.notify(order)
        else:
            print(data)

        return order

    def _makeorder(self, action, owner, data=None,
                   symbol=None,
                   size=None, price=None, plimit=None,
                   exectype=None, valid=None,
                   tradeid=0, **kwargs):

        order = futuOrder(action, owner=owner, data=data,
                          symbol=symbol,
                          size=size, price=price, pricelimit=plimit,
                          exectype=exectype, valid=valid,
                          tradeid=tradeid, simulated=True,
                          **kwargs)

        # order.addcomminfo(self.getcommissioninfo(data))
        return order

    def buy(self, owner=None, data=None,
            symbol=None,
            size=None, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            parent=None, transmit=True,
            **kwargs):

        order = self._makeorder(
            TrdSide.BUY, owner, data=data,
            symbol=symbol,
            size=size, price=price,
            plimit=plimit, exectype=exectype,
            valid=valid, tradeid=tradeid,
            **kwargs)

        return self.submit(order)

    def sell(self, owner=None, data=None,
             symbol=None,
             size=None, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             parent=None, transmit=True,
             **kwargs):

        order = self._makeorder(
            TrdSide.SELL, owner, data=data,
            symbol=symbol,
            size=size, price=price,
            plimit=plimit, exectype=exectype,
            valid=valid, tradeid=tradeid,
            **kwargs)

        return self.submit(order)

    def cancel(self, order):
        o = self.orders[order.ref]
        if order.status == Order.Cancelled:  # already cancelled
            return

        return self.futu.order_cancel(order)

    def notify(self, order):
        self.notifs.append(order.clone())

    def get_notification(self):
        if not self.notifs:
            return None

        return self.notifs.popleft()

    def next(self):
        self.notifs.append(None)  # mark notification boundary
