import time

from rabbitmq_connect import Manager

manager = Manager()
connection = manager.get_connect()
channel = connection.channel()
channel.queue_declare(queue='work_queue')


def callback(ch, method, properties, body):
    print(f'recv message >>> {body}')
    time.sleep(3)
    print(f'{body} task done')
    ch.basic_ack(delivery_tag=method.delivery_tag)  # 手动 ack


# 定义消息的处理数量, 指明当消费者中有 n 个消息未发送确认信息时，生产者就停止向此消费者发送消息
channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue='work_queue',
    on_message_callback=callback,
    auto_ack=False
)
channel.start_consuming()
