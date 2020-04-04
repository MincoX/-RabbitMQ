import datetime

from rabbitmq_connect import Manager

manager = Manager(username='MincoX', password='mincoroot', host='49.232.19.51', port=5672, virtual_host='/')
connection = manager.get_connect()

# 创建通道
channel = connection.channel()
# 声明创建的 channel
channel.queue_declare(queue='simple')

# 向队列中添加消息
message = f'hello world | {datetime.datetime.now()}'
channel.basic_publish(
    exchange='',  # 使用 mq 默认交换机（消息转发器）
    routing_key='simple',  # 根据指定的 key 向所有连接交换机的所匹配的 key 的队列进行发消息
    body=f'{message}'  # 发送的消息
)
print(f'发送信息 >>> {message}')
channel.close()
