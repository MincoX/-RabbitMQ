from rabbitmq_connect import Manager

manager = Manager()
connection = manager.get_connect()

channel = connection.channel()
channel.exchange_declare(
    exchange='direct_logs',
    exchange_type='direct'
)

# 生成随机接收队列
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# 接收监听参数
severities = ['info', 'warning', 'error']

# 一个队列可以绑定多个 routing key， 循环对队列进行 routing key 绑定
for severity in severities:
    channel.queue_bind(
        exchange='direct_logs',
        queue=queue_name,
        routing_key=severity
    )

print(' [*] Waiting for logs. To exit press CTRL+C')


# 定义回调参数
def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))


# 定义通道参数
channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=True
)

# 开始接收消息
channel.start_consuming()
