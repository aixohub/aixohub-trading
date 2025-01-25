# 创建生产者配置
import json

from clickhouse_driver import Client

conf = {
    'bootstrap.servers': 'www.aixohub.com:9092',
    'group.id': 'consumer-clickhouse',
    'auto.offset.reset': 'earliest'

}

from confluent_kafka import Consumer


# 发送消息
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


clickhouse_host = '10.0.0.12'
clickhouse_user = "default"
clickhouse_pwd = "jupyternote2026"
clickhouse_db = 'default'

clickHouseClient = Client(host=clickhouse_host, user=clickhouse_user, password=clickhouse_pwd,
                                       database=clickhouse_db)

if __name__ == '__main__':
    c = Consumer(conf)

    # 发送消息
    topic = 'stock-rgti'
    c.subscribe([topic])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        msgs = msg.value().decode('utf-8')
        v = json.loads(msgs)
        datetime = v['time']
        symbol = v['symbol']
        bidSize = v['bidSize']
        askSize = v['askSize']
        bidPrice = v['bidPrice']
        askPrice = v['askPrice']
        tableName = 'stock_{}'.format(symbol)
        sql = f"""INSERT INTO {tableName} (symbol, datetime,  open, close, volume, bid_price, bid_size, ask_price, ask_size) VALUES ('{symbol}', '{datetime}',{bidPrice},{askPrice},{bidSize},{bidPrice},{bidSize},{askPrice},{askSize}); """

        print(sql)
        clickHouseClient.execute(msgs)
        print('Received message: {}'.format(msgs))

    c.close()
