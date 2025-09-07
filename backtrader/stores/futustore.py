import collections
import queue
import threading

from futu import SecurityFirm, TrdMarket, OpenSecTradeContext, RET_OK

from backtrader import with_metaclass, Position
from backtrader.store import MetaSingleton


class FutuStore(with_metaclass(MetaSingleton, object)):

    DataCls = None  # data class will auto register

    params = (
        ('token', ''),
        ('account', ''),
        ('trdMarket', TrdMarket.HK),
        ('host', '127.0.0.1'),
        ('port', 11111),
        ('security_firm', SecurityFirm.FUTUSECURITIES),
        ('practice', False),
        ('account_tmout', 10.0),  # account balance refresh timeout
    )


    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(FutuStore, self).__init__()

        self.notifs = collections.deque()  # store notifications for cerebro

        self._env = None  # reference to cerebro for general notifications
        self.broker = None  # broker instance
        self.datas = list()  # datas that have registered over start

        self._orders = collections.OrderedDict()  # map order.ref to oid
        self._ordersrev = collections.OrderedDict()  # map oid to order.ref
        self._transpend = collections.defaultdict(collections.deque)

        self.positions = collections.defaultdict(Position)

        self._env = None
        self.trd_ctx =  OpenSecTradeContext(filter_trdmarket=self.p.trdMarket, host=self.p.host, port=self.p.port, security_firm=self.p.security_firm)


        self._cash = 0.0
        self._value = 0.0
        self._evt_acct = threading.Event()

    def start(self, data=None, broker=None):
        # Datas require some processing to kickstart data reception
        if data is None and broker is None:
            self.cash = None
            return

        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            if self.broker is not None:
                self.broker.data_started(data)

        elif broker is not None:
            self.broker = broker
            self.broker_threads()

    def stop(self):
        # signal end of thread
        if self.broker is not None:
            self.q_account.put(None)

    def broker_threads(self):
        self.q_account = queue.Queue()
        self.q_account.put(True)  # force an immediate update
        t = threading.Thread(target=self._t_account)
        t.daemon = True
        t.start()

        # Wait once for the values to be set
        self._evt_acct.wait(self.p.account_tmout)

    def _t_account(self):
        print("")

    def _get_account_info(self):
        ret, data = self.trd_ctx.accinfo_query()
        if ret == RET_OK:
            self._cash = data['us_cash'][0]
            self._value = data['total_assets'][0]


    def get_cash(self):
        self._get_account_info()
        return self._cash

    def get_value(self):
        self._get_account_info()
        return self._value

    def get_positions(self):
        ret, positions = self.trd_ctx.position_list_query()
        if ret == RET_OK:
            if positions.shape[0] > 0:
                for contract in positions.to_dict(orient="records"):
                    position = Position(float(contract['qty']), float(contract['cost_price']), contract['code'], contract['stock_name'], contract['currency'], contract['market_val'])
                    self.positions[contract['code']] = position

        return  self.positions