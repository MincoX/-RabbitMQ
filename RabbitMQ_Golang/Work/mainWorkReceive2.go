package main

import "Rabbitmq/Connections"

/*
simple 模式的消费者
	1. 声明队列
	2. 定义接收消息的方法
	3. 开启协程阻塞
*/

func main() {
	rabbitmq := Connections.NewRabbitMqSimple("goWork")
	rabbitmq.ConsumeSimple()
}
