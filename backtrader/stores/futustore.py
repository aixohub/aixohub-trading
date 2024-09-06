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
import inspect
import itertools
import threading
import time
from bisect import bisect
from copy import copy
from datetime import datetime, timedelta

import pandas as pd

pd.options.display.max_rows = 5000
pd.options.display.max_columns = 5000
pd.options.display.width = 1000

from futu import OpenSecTradeContext, SecurityFirm, TrdMarket, ModifyOrderOp, OpenFutureTradeContext, OpenQuoteContext, \
    RET_OK

from backtrader import TimeFrame, Position
from backtrader.metabase import MetaParams
from backtrader.utils import AutoDict, UTC
from backtrader.utils.py3 import bytes, bstr, queue, with_metaclass, long

bytes = bstr  # py2/3 need for ibpy


def _ts2dt(tstamp=None):
    # Transforms a RTVolume timestamp to a datetime object
    if not tstamp:
        return datetime.utcnow()

    sec, msec = divmod(long(tstamp), 1000)
    usec = msec * 1000
    return datetime.utcfromtimestamp(sec).replace(microsecond=usec)


class RTVolume(object):
    '''Parses a tickString tickType 48 (RTVolume) event from the IB API into its
    constituent fields

    Supports using a "price" to simulate an RTVolume from a tickPrice event
    '''
    _fields = [
        ('price', float),
        ('size', int),
        ('datetime', _ts2dt),
        ('volume', int),
        ('vwap', float),
        ('single', bool)
    ]

    def __init__(self, rtvol='', price=None, tmoffset=None):
        # Use a provided string or simulate a list of empty tokens
        tokens = iter(rtvol.split(';'))

        # Put the tokens as attributes using the corresponding func
        for name, func in self._fields:
            setattr(self, name, func(next(tokens)) if rtvol else func())

        # If price was provided use it
        if price is not None:
            self.price = price

        if tmoffset is not None:
            self.datetime += tmoffset


class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''

    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


# Decorator to mark methods to register with ib.opt
def futuregister(f):
    f._futuregister = True
    return f


class FutuStore(with_metaclass(MetaSingleton, object)):
    '''Singleton class wrapping an ibpy ibConnection instance.

    '''

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    params = (
        ('host', '127.0.0.1'),
        ('port', 11111),
        ('market', TrdMarket.HK),
        ('trade_passwd', None),
        ('clientId', None),  # None generates a random clientid 1 -> 2^16
        ('notifyall', False),
        ('_debug', False),
        ('reconnect', 3),  # -1 forever, 0 No, > 0 number of retries
        ('timeout', 3.0),  # timeout between reconnections
        ('timeoffset', True),  # Use offset to server for timestamps if needed
        ('timerefresh', 60.0),  # How often to refresh the timeoffset
        ('indcash', True),  # Treat IND codes as CASH elements
    )

    @classmethod
    def getdata(cls, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(FutuStore, self).__init__()

        self._lock_q = threading.Lock()  # sync access to _tickerId/Queues
        self._lock_accupd = threading.Lock()  # sync account updates
        self._lock_pos = threading.Lock()  # sync account updates
        self._lock_notif = threading.Lock()  # sync access to notif queue

        # Account list received
        self._event_managed_accounts = threading.Event()
        self._event_accdownload = threading.Event()

        self.dontreconnect = False  # for non-recoverable connect errors

        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start
        self.ccount = 0  # requests to start (from cerebro or datas)

        self._lock_tmoffset = threading.Lock()
        self.tmoffset = timedelta()  # to control time difference with server

        # Structures to hold datas requests
        self.qs = collections.OrderedDict()  # key: tickerId -> queues
        self.ts = collections.OrderedDict()  # key: queue -> tickerId
        self.iscash = dict()  # tickerIds from cash products (for ex: EUR.JPY)

        self.histexreq = dict()  # holds segmented historical requests
        self.histfmt = dict()  # holds datetimeformat for request
        self.histsend = dict()  # holds sessionend (data time) for request
        self.histtz = dict()  # holds sessionend (data time) for request

        self.acc_cash = AutoDict()  # current total cash per account
        self.acc_value = AutoDict()  # current total value per account
        self.acc_upds = AutoDict()  # current account valueinfos per account

        self.port_update = False  # indicate whether to signal to broker

        self.positions = collections.defaultdict(Position)  # actual positions

        self.orderid = None  # next possible orderid (will be itertools.count)

        self.cdetails = collections.defaultdict(list)  # hold cdetails requests

        self.managed_accounts = list()  # received via managedAccounts

        self.notifs = queue.Queue()  # store notifications for cerebro

        self.trade_passwd = '123456'
        # futu connection object
        self.conn = FutuTradeContext(env=self._env, market=TrdMarket.HK, host=self.p.host,
                                     port=self.p.port, trade_passwd=self.trade_passwd)

        # Register decorated methods with the conn
        methods = inspect.getmembers(self, inspect.ismethod)
        for name, method in methods:
            if not getattr(method, '_futuregister', False):
                continue

        # This utility key function transforms a barsize into a:
        #   (Timeframe, Compression) tuple which can be sorted
        def keyfn(x):
            n, t = x.split()
            tf, comp = self._sizes[t]
            return (tf, int(n) * comp)

        # This utility key function transforms a duration into a:
        #   (Timeframe, Compression) tuple which can be sorted
        def key2fn(x):
            n, d = x.split()
            tf = self._dur2tf[d]
            return (tf, int(n))

        # Generate a table of reverse durations
        self.revdur = collections.defaultdict(list)
        # The table (dict) is a ONE to MANY relation of
        #   duration -> barsizes
        # Here it is reversed to get a ONE to MANY relation of
        #   barsize -> durations
        for duration, barsizes in self._durations.items():
            for barsize in barsizes:
                self.revdur[keyfn(barsize)].append(duration)

        # Once managed, sort the durations according to real duration and not
        # to the text form using the utility key above
        for barsize in self.revdur:
            self.revdur[barsize].sort(key=key2fn)

    def start(self, data=None, broker=None):
        self.reconnect(fromstart=True)  # reconnect should be an invariant

        # Datas require some processing to kickstart data reception
        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            # if connection fails, get a fake registration that will force the
            # datas to try to reconnect or else bail out
            return self.getTickerQueue(start=True)

        elif broker is not None:
            self.broker = broker

    def stop(self):
        try:
            self.conn.disconnect()  # disconnect should be an invariant
        except AttributeError:
            pass  # conn may have never been connected and lack "disconnect"

        # Unblock any calls set on these events
        self._event_managed_accounts.set()
        self._event_accdownload.set()

    def logmsg(self, *args):
        # for logging purposes
        if self.p._debug:
            print(*args)

    def watcher(self, msg):
        # will be registered to see all messages if debug is requested
        self.logmsg(str(msg))
        if self.p.notifyall:
            self.notifs.put((msg, tuple(msg.values()), dict(msg.items())))

    def connected(self):
        # The isConnected method is available through __getattr__ indirections
        # and may not be present, which indicates that no connection has been
        # made because the subattribute sender has not yet been created, hence
        # the check for the AttributeError exception
        try:
            return self.conn.isConnected
        except AttributeError:
            pass

        return False  # non-connected (including non-initialized)

    def reconnect(self, fromstart=False, resub=False):
        # This method must be an invariant in that it can be called several
        # times from the same source and must be consistent. An exampler would
        # be 5 datas which are being received simultaneously and all request a
        # reconnect

        # Policy:
        #  - if dontreconnect has been set, no option to connect is possible
        #  - check connection and use the absence of isConnected as signal of
        #    first ever connection (add 1 to retries too)
        #  - Calculate the retries (forever or not)
        #  - Try to connct
        #  - If achieved and fromstart is false, the datas will be
        #    re-kickstarted to recreate the subscription
        firstconnect = False
        try:
            if self.conn.isConnected:
                return True  # nothing to do
        except AttributeError:
            # Not connected, several __getattr__ indirections to
            # self.conn.sender.client.isConnected
            firstconnect = True

        if self.dontreconnect:
            return False

        # This is only invoked from the main thread by datas and therefore no
        # lock is needed to control synchronicity to it
        retries = self.p.reconnect
        if retries >= 0:
            retries += firstconnect

        while retries < 0 or retries:
            if not firstconnect:
                time.sleep(self.p.timeout)

            firstconnect = False

            if self.conn.connect():
                if not fromstart or resub:
                    self.startdatas()
                return True  # connection successful

            if retries > 0:
                retries -= 1

        self.dontreconnect = True
        return False  # connection/reconnection failed

    def startdatas(self):
        # kickstrat datas, not returning until all of them have been done
        ts = list()
        for data in self.datas:
            t = threading.Thread(target=data.reqdata)
            t.start()
            ts.append(t)

        for t in ts:
            t.join()

    def stopdatas(self):
        # stop subs and force datas out of the loop (in LIFO order)
        qs = list(self.qs.values())
        ts = list()
        for data in self.datas:
            t = threading.Thread(target=data.canceldata)
            t.start()
            ts.append(t)

        for t in ts:
            t.join()

        for q in reversed(qs):  # datamaster the last one to get a None
            q.put(None)

    def get_notifications(self):
        '''Return the pending "store" notifications'''
        # The background thread could keep on adding notifications. The None
        # mark allows to identify which is the last notification to deliver
        self.notifs.put(None)  # put a mark
        notifs = list()
        while True:
            notif = self.notifs.get()
            if notif is None:  # mark is reached
                break
            notifs.append(notif)

        return notifs

    @futuregister
    def error(self, msg):
        # 100-199 Order/Data/Historical related
        # 200-203 tickerId and Order Related
        # 300-399 A mix of things: orders, connectivity, tickers, misc errors
        # 400-449 Seem order related again
        # 500-531 Connectivity/Communication Errors
        # 10000-100027 Mix of special orders/routing
        # 1100-1102 TWS connectivy to the outside
        # 1300- Socket dropped in client-TWS communication
        # 2100-2110 Informative about Data Farm status (id=-1)

        # All errors are logged to the environment (cerebro), because many
        # errors in Interactive Brokers are actually informational and many may
        # actually be of interest to the user
        if not self.p.notifyall:
            self.notifs.put((msg, tuple(msg.values()), dict(msg.items())))

        # Manage those events which have to do with connection
        if msg.errorCode is None:
            # Usually received as an error in connection of just before disconn
            pass
        elif msg.errorCode in [200, 203, 162, 320, 321, 322]:
            # cdetails 200 security not found, notify over right queue
            # cdetails 203 security not allowed for acct
            try:
                q = self.qs[msg.id]
            except KeyError:
                pass  # should not happend but it can
            else:
                self.cancelQueue(q, True)

        elif msg.errorCode in [354, 420]:
            # 354 no subscription, 420 no real-time bar for contract
            # the calling data to let the data know ... it cannot resub
            try:
                q = self.qs[msg.id]
            except KeyError:
                pass  # should not happend but it can
            else:
                q.put(-msg.errorCode)
                self.cancelQueue(q)

        elif msg.errorCode == 10225:
            # 10225-Bust event occurred, current subscription is deactivated.
            # Please resubscribe real-time bars immediately.
            try:
                q = self.qs[msg.id]
            except KeyError:
                pass  # should not happend but it can
            else:
                q.put(-msg.errorCode)

        elif msg.errorCode == 326:  # not recoverable, clientId in use
            self.dontreconnect = True
            self.conn.disconnect()
            self.stopdatas()

        elif msg.errorCode == 502:
            # Cannot connect to TWS: port, config not open, tws off (504 then)
            self.conn.disconnect()
            self.stopdatas()

        elif msg.errorCode == 504:  # Not Connected for data op
            # Once for each data
            pass  # don't need to manage it

        elif msg.errorCode == 1300:
            # TWS has been closed. The port for a new connection is there
            # newport = int(msg.errorMsg.split('-')[-1])  # bla bla bla -7496
            self.conn.disconnect()
            self.stopdatas()

        elif msg.errorCode == 1100:
            # Connection lost - Notify ... datas will wait on the queue
            # with no messages arriving
            for q in self.ts:  # key: queue -> ticker
                q.put(-msg.errorCode)

        elif msg.errorCode == 1101:
            # Connection restored and tickerIds are gone
            for q in self.ts:  # key: queue -> ticker
                q.put(-msg.errorCode)

        elif msg.errorCode == 1102:
            # Connection restored and tickerIds maintained
            for q in self.ts:  # key: queue -> ticker
                q.put(-msg.errorCode)

        elif msg.errorCode < 500:
            # Given the myriad of errorCodes, start by assuming is an order
            # error and if not, the checks there will let it go
            if msg.id < self.REQIDBASE:
                if self.broker is not None:
                    self.broker.push_ordererror(msg)
            else:
                # Cancel the queue if a "data" reqId error is given: sanity
                q = self.qs[msg.id]
                self.cancelQueue(q, True)

    @futuregister
    def connectionClosed(self, msg):
        # Sometmes this comes without 1300/502 or any other and will not be
        # seen in error hence the need to manage the situation independently
        self.conn.disconnect()
        self.stopdatas()

    @futuregister
    def managedAccounts(self, msg):
        # 1st message in the stream
        self.managed_accounts = msg.accountsList.split(',')
        self._event_managed_accounts.set()

        # Request time to avoid synchronization issues
        self.reqCurrentTime()

    def reqCurrentTime(self):
        self.conn.reqCurrentTime()

    @futuregister
    def currentTime(self, msg):
        if not self.p.timeoffset:  # only if requested ... apply timeoffset
            return
        curtime = datetime.fromtimestamp(float(msg.time))
        with self._lock_tmoffset:
            self.tmoffset = curtime - datetime.now()

        threading.Timer(self.p.timerefresh, self.reqCurrentTime).start()

    def timeoffset(self):
        with self._lock_tmoffset:
            return self.tmoffset



    @futuregister
    def nextValidId(self, msg):
        # Create a counter from the TWS notified value to apply to orders
        self.orderid = itertools.count(msg.orderId)

    def nextOrderId(self):
        # Get the next ticker using next on the itertools.count made with the
        # notified value from TWS
        return next(self.orderid)

    def reuseQueue(self, tickerId):
        '''Reuses queue for tickerId, returning the new tickerId and q'''
        with self._lock_q:
            # Invalidate tickerId in qs (where it is a key)
            q = self.qs.pop(tickerId, None)  # invalidate old
            iscash = self.iscash.pop(tickerId, None)

            # Update ts: q -> ticker
            tickerId = self.nextTickerId()  # get new tickerId
            self.ts[q] = tickerId  # Update ts: q -> tickerId
            self.qs[tickerId] = q  # Update qs: tickerId -> q
            self.iscash[tickerId] = iscash

        return tickerId, q

    def getTickerQueue(self, start=False):
        '''Creates ticker/Queue for data delivery to a data feed'''
        q = queue.Queue()
        if start:
            q.put(None)
            return q

        with self._lock_q:
            tickerId = self.nextTickerId()
            self.qs[tickerId] = q  # can be managed from other thread
            self.ts[q] = tickerId
            self.iscash[tickerId] = False

        return tickerId, q

    def cancelQueue(self, q, sendnone=False):
        '''Cancels a Queue for data delivery'''
        # pop ts (tickers) and with the result qs (queues)
        tickerId = self.ts.pop(q, None)
        self.qs.pop(tickerId, None)

        self.iscash.pop(tickerId, None)

        if sendnone:
            q.put(None)

    def validQueue(self, q):
        '''Returns (bool)  if a queue is still valid'''
        return q in self.ts  # queue -> ticker

    def getContractDetails(self, contract, maxcount=None):
        cds = list()
        q = self.reqContractDetails(contract)
        while True:
            msg = q.get()
            if msg is None:
                break
            cds.append(msg)

        if not cds or (maxcount and len(cds) > maxcount):
            err = 'Ambiguous contract: none/multiple answers received'
            self.notifs.put((err, cds, {}))
            return None

        return cds

    def reqContractDetails(self, contract):
        # get a ticker/queue for identification/data delivery
        tickerId, q = self.getTickerQueue()
        self.conn.reqContractDetails(tickerId, contract)
        return q

    @futuregister
    def contractDetailsEnd(self, msg):
        '''Signal end of contractdetails'''
        self.cancelQueue(self.qs[msg.reqId], True)

    @futuregister
    def contractDetails(self, msg):
        '''Receive answer and pass it to the queue'''
        self.qs[msg.reqId].put(msg)

    def reqHistoricalDataEx(self, contract, enddate, begindate,
                            timeframe, compression,
                            what=None, useRTH=False, tz='', sessionend=None,
                            tickerId=None):
        '''
        Extension of the raw reqHistoricalData proxy, which takes two dates
        rather than a duration, barsize and date

        It uses the IB published valid duration/barsizes to make a mapping and
        spread a historical request over several historical requests if needed
        '''
        # Keep a copy for error reporting purposes
        kwargs = locals().copy()
        kwargs.pop('self', None)  # remove self, no need to report it

        if timeframe < TimeFrame.Seconds:
            # Ticks are not supported
            return self.getTickerQueue(start=True)

        if enddate is None:
            enddate = datetime.now()

        if begindate is None:
            duration = self.getmaxduration(timeframe, compression)
            if duration is None:
                err = ('No duration for historical data request for '
                       'timeframe/compresison')
                self.notifs.put((err, (), kwargs))
                return self.getTickerQueue(start=True)
            barsize = self.tfcomp_to_size(timeframe, compression)
            if barsize is None:
                err = ('No supported barsize for historical data request for '
                       'timeframe/compresison')
                self.notifs.put((err, (), kwargs))
                return self.getTickerQueue(start=True)

            return self.reqHistoricalData(contract=contract, enddate=enddate,
                                          duration=duration, barsize=barsize,
                                          what=what, useRTH=useRTH, tz=tz,
                                          sessionend=sessionend)

        # Check if the requested timeframe/compression is supported by IB
        durations = self.getdurations(timeframe, compression)
        if not durations:  # return a queue and put a None in it
            return self.getTickerQueue(start=True)

        # Get or reuse a queue
        if tickerId is None:
            tickerId, q = self.getTickerQueue()
        else:
            tickerId, q = self.reuseQueue(tickerId)  # reuse q for old tickerId

        # Get the best possible duration to reduce number of requests
        duration = None
        for dur in durations:
            intdate = self.dt_plus_duration(begindate, dur)
            if intdate >= enddate:
                intdate = enddate
                duration = dur  # begin -> end fits in single request
                break

        if duration is None:  # no duration large enough to fit the request
            duration = durations[-1]

            # Store the calculated data
            self.histexreq[tickerId] = dict(
                contract=contract, enddate=enddate, begindate=intdate,
                timeframe=timeframe, compression=compression,
                what=what, useRTH=useRTH, tz=tz, sessionend=sessionend)

        barsize = self.tfcomp_to_size(timeframe, compression)
        self.histfmt[tickerId] = timeframe >= TimeFrame.Days
        self.histsend[tickerId] = sessionend
        self.histtz[tickerId] = tz

        if contract.m_secType in ['CASH', 'CFD']:
            self.iscash[tickerId] = 1  # msg.field code
            if not what:
                what = 'BID'  # default for cash unless otherwise specified

        elif contract.m_secType in ['IND'] and self.p.indcash:
            self.iscash[tickerId] = 4  # msg.field code

        what = what or 'TRADES'

        self.conn.reqHistoricalData(
            tickerId,
            contract,
            bytes(intdate.strftime('%Y%m%d %H:%M:%S') + ' GMT'),
            bytes(duration),
            bytes(barsize),
            bytes(what),
            int(useRTH),
            2)  # dateformat 1 for string, 2 for unix time in seconds

        return q

    def reqHistoricalData(self, contract, enddate, duration, barsize,
                          what=None, useRTH=False, tz='', sessionend=None):
        '''Proxy to reqHistorical Data'''

        # get a ticker/queue for identification/data delivery
        tickerId, q = self.getTickerQueue()

        if contract.m_secType in ['CASH', 'CFD']:
            self.iscash[tickerId] = True
            if not what:
                what = 'BID'  # TRADES doesn't work
            elif what == 'ASK':
                self.iscash[tickerId] = 2
        else:
            what = what or 'TRADES'

        # split barsize "x time", look in sizes for (tf, comp) get tf
        tframe = self._sizes[barsize.split()[1]][0]
        self.histfmt[tickerId] = tframe >= TimeFrame.Days
        self.histsend[tickerId] = sessionend
        self.histtz[tickerId] = tz

        self.conn.reqHistoricalData(
            tickerId,
            contract,
            bytes(enddate.strftime('%Y%m%d %H:%M:%S') + ' GMT'),
            bytes(duration),
            bytes(barsize),
            bytes(what),
            int(useRTH),
            2)

        return q

    def cancelHistoricalData(self, q):
        '''Cancels an existing HistoricalData request

        Params:
          - q: the Queue returned by reqMktData
        '''
        with self._lock_q:
            self.conn.cancelHistoricalData(self.ts[q])
            self.cancelQueue(q, True)

    def reqRealTimeBars(self, contract, useRTH=False, duration=5):
        '''Creates a request for (5 seconds) Real Time Bars

        Params:
          - contract: a ib.ext.Contract.Contract intance
          - useRTH: (default: False) passed to TWS
          - duration: (default: 5) passed to TWS, no other value works in 2016)

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        '''
        # get a ticker/queue for identification/data delivery
        tickerId, q = self.getTickerQueue()

        # 20150929 - Only 5 secs supported for duration
        self.conn.reqRealTimeBars(
            tickerId,
            contract,
            duration,
            bytes('TRADES'),
            int(useRTH))

        return q

    def cancelRealTimeBars(self, q):
        '''Cancels an existing MarketData subscription

        Params:
          - q: the Queue returned by reqMktData
        '''
        with self._lock_q:
            tickerId = self.ts.get(q, None)
            if tickerId is not None:
                self.conn.cancelRealTimeBars(tickerId)

            self.cancelQueue(q, True)

    def reqMktData(self, contract, what=None):
        '''Creates a MarketData subscription

        Params:
          - contract: a ib.ext.Contract.Contract intance

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        '''
        # get a ticker/queue for identification/data delivery
        tickerId, q = self.getTickerQueue()
        ticks = '233'  # request RTVOLUME tick delivered over tickString

        if contract.m_secType in ['CASH', 'CFD']:
            self.iscash[tickerId] = True
            ticks = ''  # cash markets do not get RTVOLUME
            if what == 'ASK':
                self.iscash[tickerId] = 2

        # q.put(None)  # to kickstart backfilling
        # Can request 233 also for cash ... nothing will arrive
        self.conn.reqMktData(tickerId, contract, bytes(ticks), False)
        return q

    def cancelMktData(self, q):
        '''Cancels an existing MarketData subscription

        Params:
          - q: the Queue returned by reqMktData
        '''
        with self._lock_q:
            tickerId = self.ts.get(q, None)
            if tickerId is not None:
                self.conn.cancelMktData(tickerId)

            self.cancelQueue(q, True)

    @futuregister
    def tickString(self, msg):
        # Receive and process a tickString message
        if msg.tickType == 48:  # RTVolume
            try:
                rtvol = RTVolume(msg.value)
            except ValueError:  # price not in message ...
                pass
            else:
                # Don't need to adjust the time, because it is in "timestamp"
                # form in the message
                self.qs[msg.tickerId].put(rtvol)

    @futuregister
    def tickPrice(self, msg):
        '''Cash Markets have no notion of "last_price"/"last_size" and the
        tracking of the price is done (industry de-facto standard at least with
        the IB API) following the BID price

        A RTVolume which will only contain a price is put into the client's
        queue to have a consistent cross-market interface
        '''
        # Used for "CASH" markets
        # The price field has been seen to be missing in some instances even if
        # "field" is 1
        tickerId = msg.tickerId
        fieldcode = self.iscash[tickerId]
        if fieldcode:
            if msg.field == fieldcode:  # Expected cash field code
                try:
                    if msg.price == -1.0:
                        # seems to indicate the stream is halted for example in
                        # between 23:00 - 23:15 CET for FOREX
                        return
                except AttributeError:
                    pass

                try:
                    rtvol = RTVolume(price=msg.price, tmoffset=self.tmoffset)
                    # print('rtvol with datetime:', rtvol.datetime)
                except ValueError:  # price not in message ...
                    pass
                else:
                    self.qs[tickerId].put(rtvol)

    @futuregister
    def realtimeBar(self, msg):
        '''Receives x seconds Real Time Bars (at the time of writing only 5
        seconds are supported)

        Not valid for cash markets
        '''
        # Get a naive localtime object
        msg.time = datetime.utcfromtimestamp(float(msg.time))
        self.qs[msg.reqId].put(msg)

    @futuregister
    def historicalData(self, msg):
        '''Receives the events of a historical data request'''
        # For multi-tiered downloads we'd need to rebind the queue to a new
        # tickerId (in case tickerIds are not reusable) and instead of putting
        # None, issue a new reqHistData with the new data and move formward
        tickerId = msg.reqId
        q = self.qs[tickerId]
        if msg.date.startswith('finished-'):
            self.histfmt.pop(tickerId, None)
            self.histsend.pop(tickerId, None)
            self.histtz.pop(tickerId, None)
            kargs = self.histexreq.pop(tickerId, None)
            if kargs is not None:
                self.reqHistoricalDataEx(tickerId=tickerId, **kargs)
                return

            msg.date = None
            self.cancelQueue(q)
        else:
            dtstr = msg.date  # Format when string req: YYYYMMDD[  HH:MM:SS]
            if self.histfmt[tickerId]:
                sessionend = self.histsend[tickerId]
                dt = datetime.strptime(dtstr, '%Y%m%d')
                dteos = datetime.combine(dt, sessionend)
                tz = self.histtz[tickerId]
                if tz:
                    dteostz = tz.localize(dteos)
                    dteosutc = dteostz.astimezone(UTC).replace(tzinfo=None)
                    # When requesting for example daily bars, the current day
                    # will be returned with the already happened data. If the
                    # session end were added, the new ticks wouldn't make it
                    # through, because they happen before the end of time
                else:
                    dteosutc = dteos

                if dteosutc <= datetime.utcnow():
                    dt = dteosutc

                msg.date = dt
            else:
                msg.date = datetime.utcfromtimestamp(long(dtstr))

        q.put(msg)

    # The _durations are meant to calculate the needed historical data to
    # perform backfilling at the start of a connetion or a connection is lost.
    # Using a timedelta as a key allows to quickly find out which bar size
    # bar size (values in the tuples int the dict) can be used.

    _durations = dict([
        # 60 seconds - 1 min
        ('60 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min')),

        # 120 seconds - 2 mins
        ('120 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins')),

        # 180 seconds - 3 mins
        ('180 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins')),

        # 300 seconds - 5 mins
        ('300 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins')),

        # 600 seconds - 10 mins
        ('600 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins')),

        # 900 seconds - 15 mins
        ('900 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins')),

        # 1200 seconds - 20 mins
        ('1200 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins')),

        # 1800 seconds - 30 mins
        ('1800 S',
         ('1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins')),

        # 3600 seconds - 1 hour
        ('3600 S',
         ('5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour')),

        # 7200 seconds - 2 hours
        ('7200 S',
         ('5 secs', '10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours')),

        # 10800 seconds - 3 hours
        ('10800 S',
         ('10 secs', '15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours')),

        # 14400 seconds - 4 hours
        ('14400 S',
         ('15 secs', '30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours')),

        # 28800 seconds - 8 hours
        ('28800 S',
         ('30 secs',
          '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours')),

        # 1 days
        ('1 D',
         ('1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day')),

        # 2 days
        ('2 D',
         ('2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day')),

        # 1 weeks
        ('1 W',
         ('3 mins', '5 mins', '10 mins', '15 mins',
          '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day', '1 W')),

        # 2 weeks
        ('2 W',
         ('15 mins', '20 mins', '30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day', '1 W')),

        # 1 months
        ('1 M',
         ('30 mins',
          '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
          '1 day', '1 W', '1 M')),

        # 2+ months
        ('2 M', ('1 day', '1 W', '1 M')),
        ('3 M', ('1 day', '1 W', '1 M')),
        ('4 M', ('1 day', '1 W', '1 M')),
        ('5 M', ('1 day', '1 W', '1 M')),
        ('6 M', ('1 day', '1 W', '1 M')),
        ('7 M', ('1 day', '1 W', '1 M')),
        ('8 M', ('1 day', '1 W', '1 M')),
        ('9 M', ('1 day', '1 W', '1 M')),
        ('10 M', ('1 day', '1 W', '1 M')),
        ('11 M', ('1 day', '1 W', '1 M')),

        # 1+ years
        ('1 Y', ('1 day', '1 W', '1 M')),
    ])

    # Sizes allow for quick translation from bar sizes above to actual
    # timeframes to make a comparison with the actual data
    _sizes = {
        'secs': (TimeFrame.Seconds, 1),
        'min': (TimeFrame.Minutes, 1),
        'mins': (TimeFrame.Minutes, 1),
        'hour': (TimeFrame.Minutes, 60),
        'hours': (TimeFrame.Minutes, 60),
        'day': (TimeFrame.Days, 1),
        'W': (TimeFrame.Weeks, 1),
        'M': (TimeFrame.Months, 1),
    }

    _dur2tf = {
        'S': TimeFrame.Seconds,
        'D': TimeFrame.Days,
        'W': TimeFrame.Weeks,
        'M': TimeFrame.Months,
        'Y': TimeFrame.Years,
    }

    def getdurations(self, timeframe, compression):
        key = (timeframe, compression)
        if key not in self.revdur:
            return []

        return self.revdur[key]

    def getmaxduration(self, timeframe, compression):
        key = (timeframe, compression)
        try:
            return self.revdur[key][-1]
        except (KeyError, IndexError):
            pass

        return None

    def tfcomp_to_size(self, timeframe, compression):
        if timeframe == TimeFrame.Months:
            return '{} M'.format(compression)

        if timeframe == TimeFrame.Weeks:
            return '{} W'.format(compression)

        if timeframe == TimeFrame.Days:
            if not compression % 7:
                return '{} W'.format(compression // 7)

            return '{} day'.format(compression)

        if timeframe == TimeFrame.Minutes:
            if not compression % 60:
                hours = compression // 60
                return ('{} hour'.format(hours)) + ('s' * (hours > 1))

            return ('{} min'.format(compression)) + ('s' * (compression > 1))

        if timeframe == TimeFrame.Seconds:
            return '{} secs'.format(compression)

        # Microseconds or ticks
        return None

    def dt_plus_duration(self, dt, duration):
        size, dim = duration.split()
        size = int(size)
        if dim == 'S':
            return dt + timedelta(seconds=size)

        if dim == 'D':
            return dt + timedelta(days=size)

        if dim == 'W':
            return dt + timedelta(days=size * 7)

        if dim == 'M':
            month = dt.month - 1 + size  # -1 to make it 0 based, readd below
            years, month = divmod(month, 12)
            return dt.replace(year=dt.year + years, month=month + 1)

        if dim == 'Y':
            return dt.replace(year=dt.year + size)

        return dt  # could do nothing with it ... return it intact

    def calcduration(self, dtbegin, dtend):
        '''Calculate a duration in between 2 datetimes. Returns single size'''
        duration, sizes = self._calcdurations(dtbegin, dtend)
        return duration, sizes[0]

    def histduration(self, dt1, dt2):
        # Given two dates calculates the smallest possible duration according
        # to the table from the Historical Data API limitations provided by IB
        #
        # Seconds: 'x S' (x: [60, 120, 180, 300, 600, 900, 1200, 1800, 3600,
        #                     7200, 10800, 14400, 28800])
        # Days: 'x D' (x: [1, 2]
        # Weeks: 'x W' (x: [1, 2])
        # Months: 'x M' (x: [1, 11])
        # Years: 'x Y' (x: [1])

        td = dt2 - dt1  # get a timedelta for calculations

        # First: array of secs
        tsecs = td.total_seconds()
        secs = [60, 120, 180, 300, 600, 900, 1200, 1800, 3600, 7200, 10800,
                14400, 28800]

        idxsec = bisect.bisect_left(secs, tsecs)
        if idxsec < len(secs):
            return '{} S'.format(secs[idxsec])

        tdextra = bool(td.seconds or td.microseconds)  # over days/weeks

        # Next: 1 or 2 days
        days = td.days + tdextra
        if td.days <= 2:
            return '{} D'.format(days)

        # Next: 1 or 2 weeks
        weeks, d = divmod(td.days, 7)
        weeks += bool(d or tdextra)
        if weeks <= 2:
            return '{} W'.format(weeks)

        # Get references to dt components
        y2, m2, d2 = dt2.year, dt2.month, dt2.day
        y1, m1, d1 = dt1.year, dt1.month, dt2.day

        H2, M2, S2, US2 = dt2.hour, dt2.minute, dt2.second, dt2.microsecond
        H1, M1, S1, US1 = dt1.hour, dt1.minute, dt1.second, dt1.microsecond

        # Next: 1 -> 11 months (11 incl)
        months = (y2 * 12 + m2) - (y1 * 12 + m1) + (
                (d2, H2, M2, S2, US2) > (d1, H1, M1, S1, US1))
        if months <= 1:  # months <= 11
            return '1 M'  # return '{} M'.format(months)
        elif months <= 11:
            return '2 M'  # cap at 2 months to keep the table clean

        # Next: years
        # y = y2 - y1 + (m2, d2, H2, M2, S2, US2) > (m1, d1, H1, M1, S1, US1)
        # return '{} Y'.format(y)

        return '1 Y'  # to keep the table clean

    def cancelOrder(self, orderid):
        '''Proxy to cancelOrder'''
        self.conn.trade_context.modify_order(ModifyOrderOp.CANCEL, orderid, 0, 0)

    def modify_order(self, orderid, qty, price):
        '''Proxy to modify_order'''
        self.conn.trade_context.modify_order(ModifyOrderOp.NORMAL, orderid, qty, price)

    def placeOrder(self, price, qty, code, trd_side):
        '''Proxy to placeOrder'''
        self.conn.trade_context.place_order(price=price, qty=qty, code=code, trd_side=trd_side, trd_env=self._env)

    @futuregister
    def openOrder(self, msg):
        '''Receive the event ``openOrder`` events'''
        self.broker.push_orderstate(msg)

    @futuregister
    def execDetails(self, msg):
        '''Receive execDetails'''
        self.broker.push_execution(msg.execution)

    @futuregister
    def orderStatus(self, msg):
        '''Receive the event ``orderStatus``'''
        self.broker.push_orderstatus(msg)

    @futuregister
    def commissionReport(self, msg):
        '''Receive the event commissionReport'''
        self.broker.push_commissionreport(msg.commissionReport)

    def reqPositions(self, code):
        '''Proxy to reqPositions'''
        self.conn.trade_context.position_list_query(code=code)

    @futuregister
    def position(self, msg):
        '''Receive event positions'''
        pass  # Not implemented yet

    @futuregister
    def updatePortfolio(self, msg):
        # Lock access to the position dicts. This is called in sub-thread and
        # can kick in at any time
        with self._lock_pos:
            if not self._event_accdownload.is_set():  # 1st event seen
                position = Position(msg.position, msg.averageCost)
                self.positions[msg.contract.m_conId] = position
            else:
                position = self.positions[msg.contract.m_conId]
                if not position.fix(msg.position, msg.averageCost):
                    err = ('The current calculated position and '
                           'the position reported by the broker do not match. '
                           'Operation can continue, but the trades '
                           'calculated in the strategy may be wrong')

                    self.notifs.put((err, (), {}))

                # Flag signal to broker at the end of account download
                # self.port_update = True
                self.broker.push_portupdate()

    def getposition(self, contract, clone=False):
        # Lock access to the position dicts. This is called from main thread
        # and updates could be happening in the background
        with self._lock_pos:
            position = self.positions[contract.m_conId]
            if clone:
                return copy(position)

            return position

    def get_acc_values(self, account=None):
        '''Returns all account value infos sent by TWS during regular updates
        Waits for at least 1 successful download

        If ``account`` is ``None`` then a dictionary with accounts as keys will
        be returned containing all accounts

        If account is specified or the system has only 1 account the dictionary
        corresponding to that account is returned
        '''
        # Wait for at least 1 account update download to have been finished
        # before the account infos can be returned to the calling client
        if self.connected():
            self._event_accdownload.wait()
        # Lock access to acc_cash to avoid an event intefering
        with self._updacclock:
            if account is None:
                # wait for the managedAccount Messages
                if self.connected():
                    self._event_managed_accounts.wait()

                if not self.managed_accounts:
                    return self.acc_upds.copy()

                elif len(self.managed_accounts) > 1:
                    return self.acc_upds.copy()

                # Only 1 account, fall through to return only 1
                account = self.managed_accounts[0]

            try:
                return self.acc_upds[account].copy()
            except KeyError:
                pass

            return self.acc_upds.copy()

    def get_acc_value(self, account=None):
        '''Returns the net liquidation value sent by TWS during regular updates
        Waits for at least 1 successful download

        If ``account`` is ``None`` then a dictionary with accounts as keys will
        be returned containing all accounts

        If account is specified or the system has only 1 account the dictionary
        corresponding to that account is returned
        '''
        # Wait for at least 1 account update download to have been finished
        # before the value can be returned to the calling client
        if self.connected():
            self._event_accdownload.wait()
        # Lock access to acc_cash to avoid an event intefering
        with self._lock_accupd:
            if account is None:
                # wait for the managedAccount Messages
                if self.connected():
                    self._event_managed_accounts.wait()

                if not self.managed_accounts:
                    return float()

                elif len(self.managed_accounts) > 1:
                    return sum(self.acc_value.values())

                # Only 1 account, fall through to return only 1
                account = self.managed_accounts[0]

            try:
                return self.acc_value[account]
            except KeyError:
                pass

            return float()

    def get_acc_cash(self, account=None):
        '''Returns the total cash value sent by TWS during regular updates
        Waits for at least 1 successful download

        If ``account`` is ``None`` then a dictionary with accounts as keys will
        be returned containing all accounts

        If account is specified or the system has only 1 account the dictionary
        corresponding to that account is returned
        '''
        # Wait for at least 1 account update download to have been finished
        # before the cash can be returned to the calling client
        if self.connected():
            return self.conn.get_acc_cash()
        else:
            self.conn.connect()
            return self.conn.get_acc_cash()


class FutuTradeContext:
    def __init__(self, env, market, host, port, trade_passwd):
        self.env = env
        self.filter_market = market
        self.host = host
        self.port = port
        self.isConnected = False
        self.trade_passwd = trade_passwd

        self.quote_data_ctx = OpenQuoteContext(host=self.host, port=self.port, is_encrypt=None)

    def connect(self):
        self.isConnected = True

    def disconnect(self):
        self.quote_data_ctx.close()
        self.isConnected = False

    def isConnected(self):
        return self.isConnected

    # def get_acc_cash(self):
    #     ret, data = self.trade_context.get_acc_list()
    #     if ret == RET_OK:
    #         acc_id = data['acc_id'][0]
    #         ret, data = self.trade_context.accinfo_query(acc_id=acc_id)
    #         if ret == RET_OK:
    #             print(data)
    #             return data['cash'][0]
    #     return float()


if __name__ == '__main__':
    trd_context = FutuTradeContext(env='', market=TrdMarket.US, host='127.0.0.1', port=11111, trade_passwd="495346")
    trd_context.connect()
    a = trd_context.get_acc_cash()
    print(a)
    trd_context.disconnect()
