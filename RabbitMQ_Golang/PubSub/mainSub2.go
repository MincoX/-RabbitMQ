package main

import "Rabbitmq/Connections"

/*
订阅 fanout 模式，消费者：
	1. 创建交换机
	2. 创建队列
	3. 队列绑定到交换机上
	4. 定义接收消息的函数
	5. 协程阻塞接收消息
*/

func main() {
	rabbitmq := Connections.NewRabbitMqPubSub("goPubSub")
	rabbitmq.ReceiveSub()
}
