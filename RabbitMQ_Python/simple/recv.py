from rabbitmq_connect import Manager

manager = Manager(username='MincoX', password='mincoroot', host='49.232.19.51', port=5672, virtual_host='/')
connection = manager.get_connect()

# 创建通道
channel = connection.channel()
""" 声明创建的 channel
虽然发送方已经声明了此队列，接收方还要再声明一遍
因为有可能接收方和发送方不在同一台机器上，此时先启动了接收方，而没有声明队列的错误发生
"""
channel.queue_declare(queue='simple')


# 定义接收到消息后执行的回调函数，此函数由 pika 库的消费者调用
def callback(ch, method, properties, body):
    """
    :param ch: channel 对象
    :param method:
    :param properties:
    :param body:
    :return:
    """
    print(f'receive message >>> {body}')


# 定义消费者
channel.basic_consume(
    queue='simple',
    on_message_callback=callback,
    auto_ack=False  # 收到消息后是否向 mq 发送确认收到信息，若 mq 没有收到确认信息，消息会保存在 mq 中，不会被删除；默认为 False，需要手动应答
)
channel.start_consuming()
