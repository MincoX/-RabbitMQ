import pika

from rabbitmq_connect import Manager

manager = Manager()

connection = manager.get_connect()
channel = connection.channel()
channel.queue_declare(queue='work_queue')

for i in range(20):
    message = f'{i} >>> hello world'
    channel.basic_publish(
        exchange='',
        routing_key='work_queue',
        body=message,
        properties=pika.BasicProperties(delivery_mode=2, )  # 标记消息持久化，需要收到消费者的确认应答后才会删除消息
    )
    print(f'send message: {message}')

channel.close()
