import pika

USERNAME = "MincoX"
PASSWORD = "mincoroot"
HOST = "49.232.19.51"
PORT = 5672
V_HOST = "py"

credentials = pika.PlainCredentials(USERNAME, PASSWORD)
parameters = pika.ConnectionParameters(host=HOST, virtual_host=V_HOST, credentials=credentials)


class RabbitMq:
    def __init__(self, queue_name, exchange_name, key, connect_parameters):
        self.connect_parameters = connect_parameters
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.key = key

        self.conn = None
        self.channel = None

    def destroy(self):
        self.channel.close()
        self.conn.close()

    def simple_produce(self):
        """

        :return:
        """
        pass

    def simple_consume(self):
        """

        :return:
        """
        pass

    def pub_sub_produce(self):
        """

        :return:
        """
        pass

    def pub_sub_consume(self):
        """

        :return:
        """
        pass

    def routing_produce(self):
        """

        :return:
        """
        pass

    def routing_consume(self):
        """

        :return:
        """
        pass

    def topic_produce(self):
        """

        :return:
        """
        pass

    def topic_consume(self):
        """

        :return:
        """
        pass


def _base(queue_name, exchange, key):
    """

    :param queue_name:
    :param exchange:
    :param key:
    :return:
    """

    rabbitmq = RabbitMq(queue_name, exchange, key, parameters)

    # rabbitmq.conn = pika.BlockingConnection(rabbitmq.connect_parameters)
    # rabbitmq.channel = rabbitmq.conn.channel()

    return rabbitmq


def new_simple(queue_name):
    """ simple 模式下只用传入队列名称
    创建 simple 模式下的 RabbitMq 实例
    :param queue_name:
    :return:
    """
    rabbitmq = _base(queue_name, "", "")

    rabbitmq.conn = pika.BlockingConnection(rabbitmq.connect_parameters)
    rabbitmq.channel = rabbitmq.conn.channel()

    return rabbitmq


def new_pub_sub(exchange):
    """ 订阅模式 fanout, 只用传入 交换机名字, routing key 无效
    创建 订阅 fanout 模式下的 RabbitMq 实例
    :param exchange:
    :return:
    """

    rabbitmq = _base("", exchange, "")

    rabbitmq.conn = pika.BlockingConnection(rabbitmq.connect_parameters)
    rabbitmq.channel = rabbitmq.conn.channel()

    return rabbitmq


def new_routing(exchange, routing_key):
    """ 订阅模式 direct, 根据绑定交换机下队列的 routing_key 进行匹配
    创建 订阅 direct 模式下的 RabbitMq 实例
    :param exchange:
    :param routing_key:
    :return:
    """
    rabbitmq = _base("", exchange, routing_key)

    rabbitmq.conn = pika.BlockingConnection(rabbitmq.connect_parameters)
    rabbitmq.channel = rabbitmq.conn.channel()

    return rabbitmq


def new_topic(exchange, routing_key):
    """ 订阅模式 topic, 根据绑定交换机下队列对 routing_key 进行模糊
    创建 订阅 topic 模式下的 RabbitMq 实例
    :param exchange:
    :param routing_key:
    :return:
    """
    rabbitmq = _base("", exchange, routing_key)

    rabbitmq.conn = pika.BlockingConnection(rabbitmq.connect_parameters)
    rabbitmq.channel = rabbitmq.conn.channel()

    return rabbitmq
