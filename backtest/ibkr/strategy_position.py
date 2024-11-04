import backtrader as bt
from backtest.ibkr.grids import SmaCross
from backtest.ibkr.grids.strategy_grid import GridStrategy
from backtrader import num2date
from backtrader.feeds import IBData

import backtrader as bt


class PriceChange(bt.Indicator):
    lines = ('price_change',)
    params = (('period', 20),)

    def __init__(self):
        self.addminperiod(self.params.period)

    def next(self):
        self.lines.price_change[0] = self.data.close[0] - self.data.close[-self.p.period]


class Acceleration(bt.Indicator):
    lines = ('acceleration',)
    params = (('period', 10),)

    def __init__(self):
        self.addminperiod(self.p.period * 2)

    def next(self):
        price_diff1 = self.data.close[0] - self.data.close[-self.p.period]
        price_diff2 = self.data.close[-self.p.period] - self.data.close[-2 * self.p.period]
        self.lines.acceleration[0] = price_diff1 - price_diff2


class VolumeMomentum(bt.Indicator):
    lines = ('volume_momentum',)
    params = (('period', 5),)

    def __init__(self):
        self.addminperiod(self.params.period)
        pass

    def next(self):
        self.lines.volume_momentum[0] = self.data.volume[0] - self.data.volume[-self.p.period]


class VWAP(bt.Indicator):
    lines = ('vwap',)
    params = (('period', 15),)

    def __init__(self):
        self.addminperiod(self.params.period)
        self.cum_price_volume = 0
        self.cum_volume = 0

    def next(self):
        self.cum_price_volume += self.data.close[0] * self.data.volume[0]
        self.cum_volume += self.data.volume[0]
        self.lines.vwap[0] = self.cum_price_volume / self.cum_volume if self.cum_volume != 0 else 0


class ExtremeReversal(bt.Indicator):
    lines = ('high_reversal', 'low_reversal')

    def __init__(self):
        self.addminperiod(1)

    def next(self):
        high = self.data.high.get(ago=0, size=1)[0]
        low = self.data.low.get(ago=0, size=1)[0]
        self.lines.high_reversal[0] = self.data.close[0] - high
        self.lines.low_reversal[0] = low - self.data.close[0]


class BlockTradeIndicator(bt.Indicator):
    lines = ('block_trade',)
    params = (('threshold', 1000),)  # 设定大单的最小值

    def next(self):
        if self.data.volume[0] >= self.p.threshold:
            direction = 1 if self.data.close[0] > self.data.close[-1] else -1
            self.lines.block_trade[0] = direction * self.data.volume[0]
        else:
            self.lines.block_trade[0] = 0


class HighLowSpread(bt.Indicator):
    lines = ('spread',)

    def next(self):
        self.lines.spread[0] = self.data.high[0] - self.data.low[0]


class IBKRPositionInitStrategy(bt.Strategy):
    params = (
        ('price_change_period', 20),
        ('volume_momentum_period', 20),
        ('acceleration_period', 20),
        ('vwap_period', 20),
        ('block_trade_threshold', 1000)
    )

    def log(self, txt, dt=None):
        ''' 日志记录函数 '''
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

    def __init__(self):
        # 初始化各因子
        self.price_change = PriceChange(period=self.p.price_change_period)
        self.volume_momentum = VolumeMomentum(period=self.p.volume_momentum_period)
        self.acceleration = Acceleration(period=self.p.acceleration_period)
        self.vwap = VWAP(period=self.p.vwap_period)
        self.high_low_spread = HighLowSpread()
        self.block_trade_indicator = BlockTradeIndicator(threshold=self.p.block_trade_threshold)

        # 使用Simple Moving Average作为均值回归的基础
        self.sma = bt.indicators.SimpleMovingAverage(self.data.close, period=15)

    def next(self):
        ts = self.data.datetime[0]
        date = num2date(self.data.datetime[0])
        acceleration = self.acceleration[0]
        symbol = self.data.contract.localSymbol
        print(
            f"""symbol: {symbol} time : {date} acceleration : {acceleration:.2f}  price_change:{self.price_change[0]:.2f}  volume_momentum:{self.volume_momentum[0]:.2f}  vwap:{self.vwap[0]:.2f} askPrice : {self.data.askPrice[0]}  bidPrice : {self.data.bidPrice[0]}  askSize : {self.data.askSize[0]} bidSize : {self.data.bidSize[0]}  """)
        # self.buy(data=self.data, symbol='nvda', size=1, price=100)
        # self.sell(data=self.data, symbol='nvda', size=1, price=100)
        # self.close()
        # 买进信号
        if self.price_change[0] > 0 and self.volume_momentum[0] > 0:
            # 价格变动和成交量都显示出动量的上升趋势
            if self.data.close[0] > self.vwap[0] and self.data.close[0] > self.sma[0]:
                # 当前价格在VWAP和均值之上，可能处于上升趋势
                if self.acceleration[0] > 0 and self.high_low_spread[0] < 0.5:
                    # 动量加速，并且波动率较低（避免追高风险）
                    # self.buy()
                    self.log("buy  price: {self.data.askPrice[0]}  ")

        # 卖出信号
        if self.price_change[0] < 0 and self.volume_momentum[0] < 0:
            # 价格和成交量的动量都转为负值
            if self.data.close[0] < self.vwap[0] and self.data.close[0] < self.sma[0]:
                # 当前价格低于VWAP和均值，可能处于下跌趋势
                if self.acceleration[0] < 0 and self.block_trade_indicator[0] < 0:
                    # 动量减速，并且有大单卖出
                    self.log(f"sell price: {self.data.askPrice[0]} ")


# api_port = 7497
api_port = 4001

if __name__ == '__main__':
    broker = bt.brokers.IBBroker(host='127.0.0.1', port=api_port, clientId=35)
    broker.start()
    cash2 = broker.getcash()

    print(f"""cash2: {cash2}""")

    position = broker.getposition(symbol="TIGR")
    print(position)
    con = {
        "code": "USD.JPY",
        "secType": "CASH",
        "what": "BID_ASK",
        "exchange": "IDEALPRO",
        "currency": "USD",
    }

    contract1 = {
        "code": "USD.JPY",
        "secType": "CASH",
        "what": "BID_ASK",
        "exchange": "IDEALPRO",
        "currency": "USD",
    }

    contract = {
        "code": "NVDA",
        "secType": "STK",
        "what": "BID_ASK",
        "exchange": "SMART",
        "currency": "USD",
    }

    # 使用自定义数据源
    data = IBData(host='127.0.0.1', port=api_port, clientId=34,
                  name=contract['code'],
                  dataname=contract['code'],
                  secType=contract['secType'],
                  what=contract['what'],
                  exchange=contract['exchange'],
                  currency=contract['currency'],
                  timeframe=bt.TimeFrame.Ticks
                  )

    # 创建 Cerebro 引擎
    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    cerebro.addindicator(Acceleration)
    # 添加策略
    cerebro.addstrategy(IBKRPositionInitStrategy)

    # 使用 IBKR 作为 broker
    cerebro.setbroker(broker)

    # 运行策略
    cerebro.run()
    # 打印最终持仓和资金
    print(f'最终资金: {cerebro.broker.getvalue():.2f}')
