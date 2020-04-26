import pika

parameters = pika.ConnectionParameters('49.232.19.51', 5672, '/', pika.PlainCredentials('MincoX', 'mincoroot'))
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='rpc_queue')


def task_a():
    return "task a done!"


def task_b():
    return "task b done!"


def on_request(chan, method, props, body):
    """执行过程函数将返回的结果通过 MQ 返回给调用者
    :param chan:
    :param method:
    :param props:
    :param body:
    :return:
    """
    print(f'rpc server receive task >>> {props}')
    task = props.app_id

    if task == 'task_a':
        resp = task_a()
    elif task == 'task_b':
        resp = task_b()

    chan.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(correlation_id=props.correlation_id),
        body=str(resp)
    )

    # 手动 ack
    chan.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    # 设置 qos 为 1，即当消费者有一个消息为消费时，队列就会停止向消费者发送消息
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume('rpc_queue', on_request)

    print("rpc server starting ... ...")
    channel.start_consuming()
