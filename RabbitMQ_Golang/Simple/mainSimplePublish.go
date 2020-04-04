package main

import (
	"Rabbitmq/Connections"
	"fmt"
)

/*
simple 模式的生产者
	1. 声明队列
	2. 向队列发送消息
*/

func main() {
	rabbitmq := Connections.NewRabbitMqSimple("goSimple")
	rabbitmq.PublishSimple("hello go simple")
	fmt.Printf("发送成功")
}
