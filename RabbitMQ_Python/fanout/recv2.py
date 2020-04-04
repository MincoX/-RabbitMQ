from rabbitmq_connect import Manager

manager = Manager()
connection = manager.get_connect()

channel = connection.channel()

# 声明交换机，名称为 logs，类型为 fanout 广播
channel.exchange_declare(
    exchange='logs',
    exchange_type="fanout"
)

# 声明队列， exclusive=True 表示将丢列的起名交给 mq 自动命名，因为这里只在意接收到信息
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# 将队列绑定到交换机上
channel.queue_bind(
    exchange='logs',
    queue=queue_name
)

print(' [*] Waiting for logs. To exit press CTRL+C')


# 定义回调函数这个回调函数将被pika库调用
def callback(ch, method, properties, body):
    print(" [x] %r" % body)


# 定义接收消息属性
channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=True
)

# 开始接收消息
channel.start_consuming()
