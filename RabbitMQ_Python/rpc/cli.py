import pika
import uuid

from rabbitmq_connect import Manager

manager = Manager()


# 定义菲波那切数列RPC Client类调用RPC Server
class FibonacciRpcClient(object):
    def __init__(self):
        self.connection = manager.get_connect()
        self.channel = self.connection.channel()
        self.response = None
        self.corr_id = str(uuid.uuid4())  # 生成任务的唯一标识

        # 生成一个随机队列-定义callback回调队列
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # 定义回调的通道属性
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,  # 回调结果执行完执行的Client端的callback方法
            auto_ack=True,
        )

    # Client端 Callback方法
    def on_response(self, ch, method, props, body):
        """
        server 端结果执行完成会自动的调用此回调函数 on_response
        :param ch:
        :param method:
        :param props:
        :param body:
        :return:
        """
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        # 发送一个消息
        self.channel.basic_publish(
            exchange='',  # 使用默认的Exchange,根据发送的routing_key来选择队列
            routing_key='queue_rpc',  # 消息发送到rpc_queue队列中
            # 定义属性
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,  # Client端定义了回调消息的callback队列
                correlation_id=self.corr_id,  # 唯一值用来做什么的？request和callback 匹配用
            ),
            body=str(n)
        )

        # 开始循环 我们刚才定义self.response=None当不为空的时候停止`
        while self.response is None:
            # 非阻塞的接受消息
            self.connection.process_data_events(time_limit=3)
        return int(self.response.decode())


fibonacci_rpc = FibonacciRpcClient()
print("Requesting fib(20)")
response = fibonacci_rpc.call(20)
print(f"get result >>> {response}")
