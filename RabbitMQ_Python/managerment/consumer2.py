from managerment.RabbitMQ import new_simple, new_broadcast, new_routing


def call_back(ch, method, properties, body):
    print(f'>>> 执行回调函数：{ch, method, properties, body}')


# 创建简单队列，简单队列默只用传入队列的名称
# mq = new_simple('test_simple')
# mq.simple_consumer(call_back=call_back)

# 创建广播模式的消费者
mq = new_broadcast('test_fanout')
mq.broadcast_consumer(call_back)
