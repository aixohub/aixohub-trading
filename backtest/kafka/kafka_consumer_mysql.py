# 创建生产者配置
import json

import mysql.connector


conf = {
    'bootstrap.servers': 'www.aixohub.com:9092',
    'group.id': 'consumer-mysql',
    'auto.offset.reset': 'earliest'

}

from confluent_kafka import Consumer


# 发送消息
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# MySQL 配置
MYSQL_HOST = 'localhost'
MYSQL_USER = 'jupyternote'
MYSQL_PASSWORD = 'jupyternote2026'
MYSQL_DATABASE = 'stock_us'

# 创建 MySQL 连接
db_connection = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)
cursor = db_connection.cursor()

if __name__ == '__main__':
    c = Consumer(conf)

    # 发送消息
    topic = 'stock-rgti'
    c.subscribe([topic])

    try:
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
            sql = f"""INSERT INTO {tableName} (symbol, datetime,  open, close, volume, bid_price, bid_size, ask_price, ask_size) VALUES
                                    ('{symbol}', '{datetime}',{bidPrice},{askPrice},{bidSize},{bidPrice},{bidSize},{askPrice},{askSize}); """
            print(sql)
            cursor.execute(sql)
            db_connection.commit()
            print('Received message: {}'.format(msgs))
    except Exception as e:
        print(e)

    finally:
        cursor.close()
        db_connection.close()
        c.close()


