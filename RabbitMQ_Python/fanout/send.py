import datetime

from rabbitmq_connect import Manager

manager = Manager()
connection = manager.get_connect()

channel = connection.channel()

# 声明 exchange（交换机）
channel.exchange_declare(
    exchange="logs",
    exchange_type="fanout"
)

"""
这里可以看到我们建立连接后,就声明了Exchange,因为把消息发送到一个不存在的Exchange是不允许的,
如果没有消费者绑定这个,Exchange消息将会被丢弃这是可以的因为没有消费者
"""
for i in range(100):
    message = f'info: Hello World! | {datetime.datetime.now()}'
    channel.basic_publish(
        exchange="logs",  # 将消息发送至Exchange为logs绑定的队列中
        routing_key="",
        body=message
    )
    print(" [x] Sent %r" % message)

# 关闭通道
connection.close()
