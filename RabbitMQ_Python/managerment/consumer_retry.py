from managerment.RabbitMQ import new_routing, get_retry_count, fail_task_handle, retry_task_handle, DELAY_MAP


def call_back(channel, method, properties, body):
    # print(f'>>> 执行回调函数：channel {channel.__dict__}, \n'
    #       f'method {method.__dict__}, \n'
    #       f'properties {properties.__dict__}, \n'
    #       f'body {body}\n')

    message = body.decode().split('.')
    count = get_retry_count(properties)
    try:
        res = 1 / int(message[0])
        print(f'业务处理成功 >>> {res}'.center(100, '*'))

    except ZeroDivisionError:
        if count < 3:
            print(f'业务处理失败, 当前次数 {count} 次，消息将发送至延迟队列 ... ...'.center(100, '*'))
            retry_task_handle(mq, channel, method, properties, body)

        else:
            print(f'业务处理失败, 当前次数 {count} 次，消息进入失败队列 ... ...'.center(100, '*'))
            fail_task_handle(mq, channel, method, properties, body)

    # 无论任务执行成功还是失败，都进行确认
    channel.basic_ack(delivery_tag=method.delivery_tag)
    # channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)


# 创建直连模式的消费者
# mq = new_routing('routingExchange', 'routingQueue', 'info')
mq = new_routing('routingExchange', 'routingQueue', ['info', 'warn'])
mq.routing_consumer(call_back)
