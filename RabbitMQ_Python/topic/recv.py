import sys

from rabbitmq_connect import Manager

manager = Manager()
connection = manager.get_connect()
channel = connection.channel()

# 声明 exchange（交换机）
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# 绑定键。‘#’匹配所有字符，‘*’匹配一个单词
binding_keys = ['[warn].*', 'info.*']

for binding_key in binding_keys:
    channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key=binding_key)

print('[*] Writing for logs. To exit press CTRL+C.')


def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))


channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

channel.start_consuming()
