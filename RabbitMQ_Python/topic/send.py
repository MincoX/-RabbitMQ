from rabbitmq_connect import Manager

manager = Manager()
connection = manager.get_connect()
channel = connection.channel()

# 声明 exchange（交换机）
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

# 这里关键字必须为点号隔开的单词，以便于消费者进行匹配。
routing_key = '[warn].kern'

message = 'Hello World!'
channel.basic_publish(exchange='topic_logs', routing_key=routing_key, body=message)

print('[生产者] Send %r:%r' % (routing_key, message))
connection.close()
