import uuid

import pika

parameters = pika.ConnectionParameters('49.232.19.51', 5672, '/', pika.PlainCredentials('MincoX', 'mincoroot'))


class RpcClient(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.callback_queue = self.channel.queue_declare("", exclusive=True).method.queue
        # 定义回调的通道属性
        self.channel.basic_consume(self.callback_queue, self.on_response, auto_ack=False)

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):

        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, name):
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                # 设置消息的回复队列
                reply_to=self.callback_queue,
                # 关联 id 用于判断消，返回的处理结果的消息是否和请求的消息一致
                correlation_id=self.corr_id,
                app_id=str(name),
            ),
            body=""
        )

        while self.response is None:
            # 非阻塞的接受消息
            self.connection.process_data_events(time_limit=3)

        return str(self.response)


if __name__ == '__main__':
    rpc = RpcClient()
    # 调用服务器上的远程函数，等待结果返回前处于阻塞状态
    task_name = 'task_b'
    response = rpc.call(task_name)
    print(f"get server response, {task_name} execute result >>> {response}")
