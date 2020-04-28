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
        self.retry_exchange = 'RetryExchange'
        self.fail_exchange = 'FailExchange'
        self.delay_queue = 'DelayQueue'
        self.fail_queue = 'FailQueue'

        self.conn = None
        self.channel = None

    def simple_producer(self, message):
        """简单队列下的生产方
        :return:
        """
        print(f'{time.time()} >>> exchange_name: {self.exchange_name}, '
              f'queue_name: {self.queue_name} send message'.center(100, '*'))

        self.channel.queue_declare(
            self.queue_name,
            # 不会创建队列，先去判断队列是否存在，若不存在则会报错
            passive=False,
            # 队列的声明默认是存放在内存中的，若 MQ 重启则会丢失，若持久化将会把队列保存到自带的数据库中，重启时会读取该数据库
            durable=True,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=False,
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

    def simple_consumer(self, call_back):
        """简单队列的消息消费者
        :return:
        """
        print(f'exchange_name: {self.exchange_name}, queue_name: {self.queue_name} start consumer'.center(100, '*'))
        # 声明队列，如果队列不存在则会自动创建，保证队列存在才可以发送消息
        self.channel.queue_declare(
            self.queue_name,
            # 不会创建队列，先去判断队列是否存在，若不存在则会报错
            passive=False,
            # 队列的声明默认是存放在内存中的，若 MQ 重启则会丢失，若持久化将会把队列保存到自带的数据库中，重启时会读取该数据库
            durable=True,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=False,
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
        print(f'{time.time()} >>> exchange_name: {self.exchange_name}, '
              f'queue_name: {self.queue_name} send message'.center(100, '*'))

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
            durable=True,
            # 当最后一个绑定在交换机上的队列删除后，自动删除此交换机
            auto_delete=False,
            # 扩展参数
            arguments=None,
        )

        # 发送消息
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

    def broadcast_consumer(self, call_back):
        """广播模式下的消费者
        :return:
        """
        print(f'exchange_name: {self.exchange_name}, queue_name: {self.queue_name} start consumer'.center(100, '*'))
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
            durable=True,
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
            durable=True,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=False,
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
        print(f'{time.time()} >>> exchange_name: {self.exchange_name}, '
              f'queue_name: {self.queue_name} send message'.center(100, '*'))

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
            durable=True,
            # 当最后一个绑定在交换机上的队列删除后，自动删除此交换机
            auto_delete=False,
            # 扩展参数
            arguments=None,
        )

        # 发送消息
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

    def routing_consumer(self, call_back):
        """直连模式的消费者
        :return:
        """
        print(f'exchange_name: {self.exchange_name}, queue_name: {self.queue_name} start consumer'.center(100, '*'))
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
            durable=True,
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
            durable=True,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=False,
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
        print(f'{time.time()} >>> exchange_name: {self.exchange_name}, '
              f'queue_name: {self.queue_name} send message'.center(100, '*'))

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
            durable=True,
            # 当最后一个绑定在交换机上的队列删除后，自动删除此交换机
            auto_delete=False,
            # 扩展参数
            arguments=None,
        )

        # 发送消息
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

    def topic_consume(self, call_back):
        """主题模式下的消费者
        :return:
        """
        print(f'exchange_name: {self.exchange_name}, queue_name: {self.queue_name} start consumer'.center(100, '*'))
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
            durable=True,
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
            durable=True,
            # 队列是否排他
            #   1. 连接关闭时，队列会自动删除；
            #   2. 是否排他，会对当前队列进行加锁，保证其他通道无法访问，适合一个队列只有一个消费者场景
            exclusive=False,
            # 当最后一个 consumer 断开后，自动的删除队列
            auto_delete=False,
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

    def init_retry_task(self):
        """创建延迟重试任务机制
        :return:
        """
        # 声明重试交换机，失败任务发送到重试交换机中
        self.channel.exchange_declare(exchange=self.retry_exchange, exchange_type='fanout', durable=True)
        # 声明失败交换机，重试次数超 3 次将消息发送到失败交换机
        self.channel.exchange_declare(exchange=self.fail_exchange, exchange_type='fanout', durable=True)

        # 声明重试队列，消息延迟过期后重新转发到原交换机下
        self.channel.queue_declare(queue=self.delay_queue, durable=True, auto_delete=False, arguments={
            'x-dead-letter-exchange': self.exchange_name,
            'x-message-ttl': 10000
        })
        # 声明失败队列，对与重试多次仍然失败的消息进入失败队列，通过邮件/短信通知管理员
        self.channel.queue_declare(queue=self.fail_queue, durable=True, auto_delete=False, arguments=None)

        # 队列绑定到对应的交换机上
        self.channel.queue_bind(self.delay_queue, self.retry_exchange, self.delay_queue)
        self.channel.queue_bind(self.fail_queue, self.fail_exchange, self.fail_queue)

    def destroy(self):
        self.channel.close()
        self.conn.close()


def new_simple(queue_name):
    """创建 simple 模式下的 RabbitMQ 实例
    simple 模式下只用传入队列名称，会使用默认的交换机 (exchange="")
    :param queue_name:
    :return:
    """

    mq = RabbitMq("", queue_name, "", parameters)

    mq.conn = pika.BlockingConnection(mq.connect_parameters)
    mq.channel = mq.conn.channel()
    mq.init_retry_task()

    return mq


def new_broadcast(exchange):
    """创建广播 fanout 模式下的 RabbitMq 实例
    订阅模式（广播模式） fanout, 会对所有绑定此交换机的队列进行消息的转发，只用传入交换机名字, routing key 无效
    :param exchange:
    :return:
    """

    mq = RabbitMq(exchange, "", "", parameters)

    mq.conn = pika.BlockingConnection(mq.connect_parameters)
    mq.channel = mq.conn.channel()
    mq.init_retry_task()

    return mq


def new_routing(exchange, queue, routing_key=""):
    """ 订阅模式 direct, 根据绑定交换机下队列的 routing_key 进行匹配
    创建 订阅 direct 模式下的 RabbitMq 实例
    :param exchange:
    :param queue:
    :param routing_key:
    :return:
    """
    if type(routing_key) is not list:
        routing_key = [] + [routing_key]

    mq = RabbitMq(exchange, queue, routing_key, parameters)

    mq.conn = pika.BlockingConnection(mq.connect_parameters)
    mq.channel = mq.conn.channel()
    mq.init_retry_task()

    return mq


def new_topic(exchange, routing_key="."):
    """订阅主题模式 topic, 将消息转发到交换机下 routing_key 模糊匹配成功的队列
    :param exchange:
    :param routing_key:
    :return:
    """
    if type(routing_key) is not list:
        routing_key = [] + [routing_key]

    mq = RabbitMq(exchange, "", routing_key, parameters)

    mq.conn = pika.BlockingConnection(mq.connect_parameters)
    mq.channel = mq.conn.channel()

    return mq


def retry_task_handle(mq, channel, method, properties, body):
    """重试任务发送到重试交换机
    :param mq:
    :param channel:
    :param method:
    :param properties:
    :param body:
    :return:
    """
    channel.basic_publish(
        mq.retry_exchange,
        routing_key=method.routing_key,
        body=body,
        mandatory=False,
        properties=pika.BasicProperties(
            delivery_mode=2,  # 消息持久化（需要在交换机、队列都进行持久化情况下消息持久化才有意义）
            headers=properties.headers
        )
    )


def fail_task_handle(mq, channel, method, properties, body):
    """超过重试次数的任务发送到失败交换机
    :param mq:
    :param channel:
    :param method:
    :param properties:
    :param body:
    :return:
    """
    channel.basic_publish(
        mq.fail_exchange,
        routing_key=method.routing_key,
        body=body,
        mandatory=False,
        properties=pika.BasicProperties(
            delivery_mode=2,  # 消息持久化（需要在交换机、队列都进行持久化情况下消息持久化才有意义）
            headers=properties.headers
        )
    )


def get_retry_count(properties):
    """从消息头中获取消息重试的次数
    :param properties:
    :return:
    """
    retry_count = 0
    headers = properties.headers
    if headers is not None:
        death = headers.get('x-death')
        if death:
            retry_count = int(death[0].get('count'))

    return retry_count


if __name__ == '__main__':
    topic_key = 'proxy.info'
    topic_keys = ['*.info', '*.warn', '*.error']
