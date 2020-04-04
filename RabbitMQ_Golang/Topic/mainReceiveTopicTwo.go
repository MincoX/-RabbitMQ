package main

import "Rabbitmq/Connections"

/*
订阅 topic 模式消费者：
	1. 声明交换机
	2. 声明队列
	3. 队列绑定交换机，指明 routing key 模糊匹配的规则
	4. 定义接收消息的方法
	5. 协程阻塞接收消息
*/

func main() {
	one := Connections.NewRabbitMqTopic("goTopic", "*.*.test2")
	one.ReceiveTopic()
}
