import pika

from rabbitmq_connect import Manager

manager = Manager()
connection = manager.get_connect()

channel = connection.channel()
channel.queue_declare(queue='queue_rpc')


def fib(n):
    """
    计算斐波那契数列
    :param n:
    :return:
    """
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


#  应答函数,它是我们接受到消息后如处理的函数替代原来的callback
def on_request(ch, method, props, body):
    n = int(body)
    print(" [.] fib(%s)" % n)
    response = fib(n)

    ch.basic_publish(
        exchange='',
        # 返回的队列,从属性的reply_to取出来
        routing_key=props.reply_to,
        # 添加correlation_id,和Client进行一致性匹配使用的
        properties=pika.BasicProperties(correlation_id=props.correlation_id),
        # 处理结果
        body=str(response)
    )
    # 增加消息回执
    ch.basic_ack(delivery_tag=method.delivery_tag)


# 公平分发
channel.basic_qos(prefetch_count=1)
# 定义接收通道的属性/定义了callback方法,接收的队列,返回的队列在哪里？on_request 的routing_key=props.reply_to
channel.basic_consume(queue='queue_rpc', on_message_callback=on_request)

# 开始接收消息
print(" [x] Awaiting RPC requests")
channel.start_consuming()
