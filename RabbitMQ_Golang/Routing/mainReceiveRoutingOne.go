package main

import "Rabbitmq/Connections"

/*
订阅 direct 模式消费者：
	1. 声明交换机
	2. 声明队列
	3. 将队列绑定交换机，指明 routing key
	4. 定义处理消息的方法
	5. 协程阻塞接收消息
*/

func main() {
	one := Connections.NewRabbitMqRouting("goRouting", "one")
	one.ReceiveRouting()

}
