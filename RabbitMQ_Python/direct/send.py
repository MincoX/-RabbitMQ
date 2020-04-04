import random

from rabbitmq_connect import Manager

manager = Manager()
connection = manager.get_connect()

# 创建通道
channel = connection.channel()
# 声明Exchange 且类型为 direct
channel.exchange_declare(
    exchange='direct_logs',
    exchange_type='direct'
)

severity_list = ['debug', 'info', 'warning', 'error']

for i in range(100):
    severity = severity_list[random.randrange(4)]
    message = f'{severity} message'

    # 发送消息指定Exchange为direct_logs,routing_key 为调用参数获取的值
    channel.basic_publish(
        exchange='direct_logs',
        routing_key=severity,
        body=message
    )
    print(f"Sent {severity}: {message}")

# 关闭通道
connection.close()
