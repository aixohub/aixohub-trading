
import backtrader as bt
from backtrader.feeds.kafkafeed import KafkaFeed


class MyStrategy(bt.Strategy):
    def __init__(self):
        # 可以定义一些指标或其他策略逻辑
        print('---s')

    def next(self):
        # 每次获取到新数据时，打印当前的收盘价
        print(f"Time: {self.data.datetime.datetime(0)}, Close: {self.data.close[0]}")


if __name__ == '__main__':
    # 创建 Cerebro 实例
    cerebro = bt.Cerebro()

    # 添加 Kafka 数据源
    kafka_feed = KafkaFeed(topic=['stock-nvda'], group_id='backtrader-nvda', bootstrap_servers=['www.aixohub.com:9092'])
    cerebro.adddata(kafka_feed)

    # 添加策略
    cerebro.addstrategy(MyStrategy)

    # 运行策略
    cerebro.run()

