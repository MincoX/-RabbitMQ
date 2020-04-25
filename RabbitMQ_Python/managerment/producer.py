from managerment.RabbitMQ import new_simple, new_broadcast, new_routing, new_topic

# 创建简单队列，简单队列默只用传入队列的名称
# mq = new_simple('test_simple')
# mq.simple_producer("test_1message")

# 创建广播模式下的生产者
# mq = new_broadcast('test_fanout')
# mq.broadcast_producer("消广播息")

# 创建直连模式下的生产者
# mq = new_routing('test_routing', 'info')
# mq = new_routing('test_routing', ['info', 'warn'])
# mq.routing_producer('message routing', 'warn')

# 创建话题模式下的生产者
mq = new_topic("test_topic", ['*.info', '*.warn', '*.error'])
mq.topic_producer('proxy info', 'proxy.info')
mq.topic_producer('wechat info', 'wechat.warn')
mq.topic_producer('miniProgram info', 'miniProgram.error')
mq.topic_producer('test info', 'miniProgram.debug')
