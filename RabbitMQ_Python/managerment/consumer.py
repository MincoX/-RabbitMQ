from managerment.RabbitMQ import new_simple, new_broadcast, new_routing, new_topic


def call_back(channel, method, properties, body):
    print(f'>>> 执行回调函数：channel {channel.__dict__}, \n'
          f'method {method.__dict__}, \n'
          f'properties {properties.__dict__}, \n'
          f'body {body}\n')

    channel.basic_ack(delivery_tag=method.delivery_tag)
    # channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)


# 创建简单队列，简单队列默只用传入队列的名称
# mq = new_simple('test_simple')
# mq.simple_consumer(call_back=call_back)

# 创建广播模式的消费者
# mq = new_broadcast('test_fanout')
# mq.broadcast_consumer(call_back)

# 创建直连模式的消费者
# mq = new_routing('routingExchange', 'routingQueue', 'info')
mq = new_routing('routingExchange', 'routingQueue', ['info', 'warn'])
mq.routing_consumer(call_back)

# 创建话题模式下的消费者
# mq = new_topic("test_topic", ['*.info', '*.warn', '*.error'])
# mq.topic_consume(call_back)
