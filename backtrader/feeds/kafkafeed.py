import json
import time
import backtrader as bt
from confluent_kafka import Consumer, KafkaException, KafkaError

class KafkaFeed(bt.feed.DataBase):
    """
    KafkaFeed: 使用 confluent-kafka 实现的 Backtrader 数据源。
    """
    params = (
        ('topic', ['topic-01']),  # Kafka topic
        ('group_id', 'consumer_backtrader'),  # Kafka 消费者组
        ('bootstrap_servers', ['localhost:9092']),  # Kafka 服务地址
        ('poll_interval', 0.1),  # 拉取 Kafka 数据的时间间隔
    )

    def __init__(self):
        # 初始化 Kafka 消费者
        self.consumer = Consumer({
            'bootstrap.servers': ','.join(self.p.bootstrap_servers),
            'group.id': self.p.group_id,
            'auto.offset.reset': 'earliest',  # 设置消费开始的偏移量
        })
        self.consumer.subscribe(self.p.topic)

        self.last_time = None
        self._next_time = time.time()

    def islive(self):
        '''Returns ``True`` to notify ``Cerebro`` that preloading and runonce
        should be deactivated'''
        return True

    def haslivedata(self):
        return True

    def _load(self):
        """
        从 Kafka 获取消息，并将其转化为 Backtrader 可以使用的格式。
        """
        # if time.time() - self._next_time < self.p.poll_interval:
        #     return False

        # 从 Kafka 消费数据
        try:
            msg = self.consumer.poll(timeout=1.0)  # 超时 1 秒钟，避免阻塞
            if msg is None:
                return False  # 如果没有数据，返回 False
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached: {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
                else:
                    raise KafkaException(msg.error())
            else:
                # 解析消息
                data = json.loads(msg.value().decode('utf-8'))

                timestamp = data['timestamp']
                open_price = data['open']
                high_price = data['high']
                low_price = data['low']
                close_price = data['close']
                volume = data['volume']

                # 将数据推送到 Backtrader
                self.lines.datetime[0] = bt.date2num(bt.num2date(timestamp))
                self.lines.open[0] = open_price
                self.lines.high[0] = high_price
                self.lines.low[0] = low_price
                self.lines.close[0] = close_price
                self.lines.volume[0] = volume

                self._next_time = time.time()  # 更新时间
                return True  # 成功加载数据

        except Exception as e:
            print(f"Error while fetching data from Kafka: {e}")
            return False

    def stop(self):
        # 关闭 Kafka 消费者
        self.consumer.close()
