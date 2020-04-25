import time

import pika

USERNAME = "MincoX"
PASSWORD = "mincoroot"
HOST = "49.232.19.51"
PORT = 5672
V_HOST = "/"

credentials = pika.PlainCredentials(USERNAME, PASSWORD)
parameters = pika.ConnectionParameters(host=HOST, virtual_host=V_HOST, credentials=credentials)


class RabbitMq:

    def __init__(self, exchange_name, queue_name, routing_key, connect_parameters):
        self.connect_parameters = connect_parameters
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.routing_key = routing_key

        self.conn = None
        self.channel = None

    def simple_producer(self, message):
        """简单队列下的生产方
        :return:
        """
        self.channel.queue_declare(
            self.queue_name,
            # 不会创建队列，先去判断队列是否存在，若不存在则会报错
            passive=False,
            # 队列的声明默认是存放在内存中的，若 MQ 重启则会丢失，若持久化将会把队列保存到自带的数据库中，重启时会读取该数据库
            durable=False,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=True,
            # 额外参数
            #   1. x-message-ttl，设置队列所有消息的统一生存时间，单位毫秒
            #   2. x-dead-letter-exchange，当消息过期或拒收时将消息推送到指定的交换机中去
            arguments=None
        )

        self.channel.basic_publish(
            # 交换器
            self.exchange_name,
            # 每声明一个队列都会默认指定 routing_key 和队列名相同，并将此队列绑定到默认交换机上
            # 路由键，将消息转发到哪一个队列的策略，默认交换机会根据 routing_key 转发
            routing_key=self.queue_name,
            # 消息体
            body=message,
            # 如果消息无法根据交换机和 RoutingKey 找到对应的队列，将会调用 basic.return 方法将消息返回个生产者，
            # 若为 False 将会把消息丢弃
            mandatory=False,
            # 为发送的消息指定属性
            properties=pika.BasicProperties(
                delivery_mode=2,  # 消息持久化（需要在交换机、队列都进行持久化情况下消息持久化才有意义）
            )
        )
        print(f'简单队列成功发送消息 >>> {message}'.center(100, '*'))

    def simple_consumer(self, call_back):
        """简单队列的消息消费者
        :return:
        """
        print('简单队列消费者开始消费消息'.center(100, '*'))
        # 声明队列，如果队列不存在则会自动创建，保证队列存在才可以发送消息
        self.channel.queue_declare(
            self.queue_name,
            # 不会创建队列，先去判断队列是否存在，若不存在则会报错
            passive=False,
            # 队列的声明默认是存放在内存中的，若 MQ 重启则会丢失，若持久化将会把队列保存到自带的数据库中，重启时会读取该数据库
            durable=False,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=True,
            # 额外参数
            #   1. x-message-ttl，设置队列所有消息的统一生存时间，单位毫秒
            #   2. x-dead-letter-exchange，当消息过期或拒收时将消息推送到指定的交换机中去
            arguments=None
        )

        # 消费队列中的任务
        self.channel.basic_consume(
            queue=self.queue_name,
            # 回调函数，处理消息的逻辑函数
            on_message_callback=call_back,
            # 是否自动确认
            auto_ack=False,
            exclusive=False,
            consumer_tag=None,
            arguments=None
        )

        self.channel.start_consuming()

    def broadcast_producer(self, message):
        """发布订阅广播模式的生产者
        :return:
        """
        # 声明交换机
        self.channel.exchange_declare(
            # 交换机名称
            exchange=self.exchange_name,
            # 交换机类型：
            #   1. fanout： 广播模式，对所有绑定到交换机上的队列进行消息转发，此时 routingKey 无效
            #   2. direct： 直连方式，根据 routingKey 转发到对于的队列
            #   3. topic： 主题模式，对 routingKey 进行模糊匹配的队列进行转发
            exchange_type='fanout',
            # 不会创建交换机，先去判断交换机是否存在，若不存在则会报错
            passive=False,
            # 持久化
            durable=False,
            # 当最后一个绑定在交换机上的队列删除后，自动删除此交换机
            auto_delete=False,
            # 扩展参数
            arguments=None,
        )

        # 发送消息
        for i in range(10):
            self.channel.basic_publish(
                # 交换器
                self.exchange_name,
                # 广播模式小 routingKey 无效
                routing_key="",
                # 消息体
                body=message,
                # 如果消息无法根据交换机和 RoutingKey 找到对应的队列，将会调用 basic.return 方法将消息返回个生产者，
                # 若为 False 将会把消息丢弃
                mandatory=False,
                # 为发送的消息指定属性
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 消息持久化（需要在交换机、队列都进行持久化情况下消息持久化才有意义）
                )
            )
            print(f'成功向广播队列发送消息 >>> {message}, {time.time()}'.center(100, '*'))
            time.sleep(2)

    def broadcast_consumer(self, call_back):
        """广播模式下的消费者
        :return:
        """
        # 声明交换机
        self.channel.exchange_declare(
            # 交换机名称
            exchange=self.exchange_name,
            # 交换机类型：
            #   1. fanout： 广播模式，对所有绑定到交换机上的队列进行消息转发，此时 routingKey 无效
            #   2. direct： 直连方式，根据 routingKey 转发到对于的队列
            #   3. topic： 主题模式，对 routingKey 进行模糊匹配的队列进行转发
            exchange_type='fanout',
            # 不会创建交换机，先去判断交换机是否存在，若不存在则会报错
            passive=False,
            # 持久化
            durable=False,
            # 当最后一个绑定在交换机上的队列删除后，自动删除此交换机
            auto_delete=False,
            # 扩展参数
            arguments=None,
        )

        # 声明队列，如果队列不存在则会自动创建，保证队列存在才可以发送消息
        self.channel.queue_declare(
            self.queue_name,
            # 不会创建队列，先去判断队列是否存在，若不存在则会报错
            passive=False,
            # 队列的声明默认是存放在内存中的，若 MQ 重启则会丢失，若持久化将会把队列保存到自带的数据库中，重启时会读取该数据库
            durable=False,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=True,
            # 额外参数
            #   1. x-message-ttl，设置队列所有消息的统一生存时间，单位毫秒
            #   2. x-dead-letter-exchange，当消息过期或拒收时将消息推送到指定的交换机中去
            arguments=None
        )

        # 将队列绑定到交换机上
        self.channel.queue_bind(
            self.queue_name,
            exchange=self.exchange_name,
        )

        # 消费队列中的任务
        self.channel.basic_consume(
            queue=self.queue_name,
            # 回调函数，处理消息的逻辑函数
            on_message_callback=call_back,
            # 是否自动确认
            auto_ack=False,
            exclusive=False,
            consumer_tag=None,
            arguments=None
        )
        self.channel.start_consuming()

    def routing_producer(self, message, routing_key=""):
        """直连模式的生产者
        :return:
        """
        # 声明交换机
        self.channel.exchange_declare(
            # 交换机名称
            exchange=self.exchange_name,
            # 交换机类型：
            #   1. fanout： 广播模式，对所有绑定到交换机上的队列进行消息转发，此时 routingKey 无效
            #   2. direct： 直连方式，根据 routingKey 转发到对于的队列
            #   3. topic： 主题模式，对 routingKey 进行模糊匹配的队列进行转发
            exchange_type='direct',
            # 不会创建交换机，先去判断交换机是否存在，若不存在则会报错
            passive=False,
            # 持久化
            durable=False,
            # 当最后一个绑定在交换机上的队列删除后，自动删除此交换机
            auto_delete=False,
            # 扩展参数
            arguments=None,
        )

        # 发送消息
        for i in range(10):
            self.channel.basic_publish(
                # 交换器
                self.exchange_name,
                # 直连模式根据绑定在交换机下队列的 routingKey 进行消息的转发
                routing_key=routing_key,
                # 消息体
                body=message,
                # 如果消息无法根据交换机和 RoutingKey 找到对应的队列，将会调用 basic.return 方法将消息返回个生产者，
                # 若为 False 将会把消息丢弃
                mandatory=False,
                # 为发送的消息指定属性
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 消息持久化（需要在交换机、队列都进行持久化情况下消息持久化才有意义）
                )
            )
            print(f'成功向直连队列发送消息 >>> {message}, {time.time()}'.center(100, '*'))
            time.sleep(2)

    def routing_consumer(self, call_back):
        """直连模式的消费者
        :return:
        """
        # 声明交换机
        self.channel.exchange_declare(
            # 交换机名称
            exchange=self.exchange_name,
            # 交换机类型：
            #   1. fanout： 广播模式，对所有绑定到交换机上的队列进行消息转发，此时 routingKey 无效
            #   2. direct： 直连方式，根据 routingKey 转发到对于的队列
            #   3. topic： 主题模式，对 routingKey 进行模糊匹配的队列进行转发
            exchange_type='direct',
            # 不会创建交换机，先去判断交换机是否存在，若不存在则会报错
            passive=False,
            # 持久化
            durable=False,
            # 当最后一个绑定在交换机上的队列删除后，自动删除此交换机
            auto_delete=False,
            # 扩展参数
            arguments=None,
        )

        # 声明队列，如果队列不存在则会自动创建，保证队列存在才可以发送消息
        self.channel.queue_declare(
            self.queue_name,
            # 不会创建队列，先去判断队列是否存在，若不存在则会报错
            passive=False,
            # 队列的声明默认是存放在内存中的，若 MQ 重启则会丢失，若持久化将会把队列保存到自带的数据库中，重启时会读取该数据库
            durable=False,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=True,
            # 额外参数
            #   1. x-message-ttl，设置队列所有消息的统一生存时间，单位毫秒
            #   2. x-dead-letter-exchange，当消息过期或拒收时将消息推送到指定的交换机中去
            arguments=None
        )

        # 将一个队列绑定多个 routing_key
        for key in self.routing_key:
            self.channel.queue_bind(
                self.queue_name,
                exchange=self.exchange_name,
                routing_key=key
            )

        # 消费队列中的任务
        self.channel.basic_consume(
            queue=self.queue_name,
            # 回调函数，处理消息的逻辑函数
            on_message_callback=call_back,
            # 是否自动确认
            auto_ack=False,
            exclusive=False,
            consumer_tag=None,
            arguments=None
        )
        self.channel.start_consuming()

    def topic_producer(self, message, routing_key="."):
        """主题模式下的生产者
        :return:
        """
        # 声明交换机
        self.channel.exchange_declare(
            # 交换机名称
            exchange=self.exchange_name,
            # 交换机类型：
            #   1. fanout： 广播模式，对所有绑定到交换机上的队列进行消息转发，此时 routingKey 无效
            #   2. direct： 直连方式，根据 routingKey 转发到对于的队列
            #   3. topic： 主题模式，对 routingKey 进行模糊匹配的队列进行转发
            exchange_type='topic',
            # 不会创建交换机，先去判断交换机是否存在，若不存在则会报错
            passive=False,
            # 持久化
            durable=False,
            # 当最后一个绑定在交换机上的队列删除后，自动删除此交换机
            auto_delete=False,
            # 扩展参数
            arguments=None,
        )

        # 发送消息
        for i in range(10):
            self.channel.basic_publish(
                # 交换器
                self.exchange_name,
                # 直连模式根据绑定在交换机下队列的 routingKey 进行消息的转发
                routing_key=routing_key,
                # 消息体
                body=message,
                # 如果消息无法根据交换机和 RoutingKey 找到对应的队列，将会调用 basic.return 方法将消息返回个生产者，
                # 若为 False 将会把消息丢弃
                mandatory=False,
                # 为发送的消息指定属性
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 消息持久化（需要在交换机、队列都进行持久化情况下消息持久化才有意义）
                )
            )
        print(f'成功向直连队列发送消息 >>> {message}, {time.time()}'.center(100, '*'))
        time.sleep(2)

    def topic_consume(self, call_back):
        """主题模式下的消费者
        :return:
        """
        # 声明交换机
        self.channel.exchange_declare(
            # 交换机名称
            exchange=self.exchange_name,
            # 交换机类型：
            #   1. fanout： 广播模式，对所有绑定到交换机上的队列进行消息转发，此时 routingKey 无效
            #   2. direct： 直连方式，根据 routingKey 转发到对于的队列
            #   3. topic： 主题模式，对 routingKey 进行模糊匹配的队列进行转发
            exchange_type='topic',
            # 不会创建交换机，先去判断交换机是否存在，若不存在则会报错
            passive=False,
            # 持久化
            durable=False,
            # 当最后一个绑定在交换机上的队列删除后，自动删除此交换机
            auto_delete=False,
            # 扩展参数
            arguments=None,
        )

        # 声明队列，如果队列不存在则会自动创建，保证队列存在才可以发送消息
        self.channel.queue_declare(
            self.queue_name,
            # 不会创建队列，先去判断队列是否存在，若不存在则会报错
            passive=False,
            # 队列的声明默认是存放在内存中的，若 MQ 重启则会丢失，若持久化将会把队列保存到自带的数据库中，重启时会读取该数据库
            durable=False,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=True,
            # 额外参数
            #   1. x-message-ttl，设置队列所有消息的统一生存时间，单位毫秒
            #   2. x-dead-letter-exchange，当消息过期或拒收时将消息推送到指定的交换机中去
            arguments=None
        )

        # 将一个队列绑定多个 routing_key
        for key in self.routing_key:
            self.channel.queue_bind(
                self.queue_name,
                exchange=self.exchange_name,
                routing_key=key
            )

        # 消费队列中的任务
        self.channel.basic_consume(
            queue=self.queue_name,
            # 回调函数，处理消息的逻辑函数
            on_message_callback=call_back,
            # 是否自动确认
            auto_ack=False,
            exclusive=False,
            consumer_tag=None,
            arguments=None
        )
        self.channel.start_consuming()

    def destroy(self):
        self.channel.close()
        self.conn.close()


def _base(exchange_name, queue_name, routing_key):
    """创建 MQ 基础类，此时 MQ 对象还未进行连接和获取 channel
    :param exchange_name:
    :param queue_name:
    :param routing_key:
    :return:
    """

    mq = RabbitMq(exchange_name, queue_name, routing_key, parameters)

    return mq


def new_simple(queue_name):
    """创建 simple 模式下的 RabbitMQ 实例
    simple 模式下只用传入队列名称，会使用默认的交换机 (exchange="")
    :param queue_name:
    :return:
    """
    mq = _base("", queue_name, "")

    mq.conn = pika.BlockingConnection(mq.connect_parameters)
    mq.channel = mq.conn.channel()

    return mq


def new_broadcast(exchange):
    """创建广播 fanout 模式下的 RabbitMq 实例
    订阅模式（广播模式） fanout, 会对所有绑定此交换机的队列进行消息的转发，只用传入交换机名字, routing key 无效
    :param exchange:
    :return:
    """

    mq = _base(exchange, "", "")

    mq.conn = pika.BlockingConnection(mq.connect_parameters)
    mq.channel = mq.conn.channel()

    return mq


def new_routing(exchange, routing_key=""):
    """ 订阅模式 direct, 根据绑定交换机下队列的 routing_key 进行匹配
    创建 订阅 direct 模式下的 RabbitMq 实例
    :param exchange:
    :param routing_key:
    :return:
    """
    if type(routing_key) is not list:
        routing_key = [] + [routing_key]

    mq = _base(exchange, "", routing_key)

    mq.conn = pika.BlockingConnection(mq.connect_parameters)
    mq.channel = mq.conn.channel()

    return mq


def new_topic(exchange, routing_key="."):
    """订阅主题模式 topic, 将消息转发到交换机下 routing_key 模糊匹配成功的队列
    :param exchange:
    :param routing_key:
    :return:
    """
    if type(routing_key) is not list:
        routing_key = [] + [routing_key]

    mq = _base(exchange, "", routing_key)

    mq.conn = pika.BlockingConnection(mq.connect_parameters)
    mq.channel = mq.conn.channel()

    return mq


if __name__ == '__main__':
    topic_key = 'proxy.info'
    topic_keys = ['*.info', '*.warn', '*.error']
