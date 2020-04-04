package main

import (
	"Rabbitmq/Connections"
	"fmt"
	"strconv"
	"time"
)

/*
订阅 topic 模式（对绑定在交换机下的 routing key 进行模糊匹配）：
	1. 声明交换机指明 topic 模式
	2. 发送消息，指明消息的 routing key
*/

func main() {
	one := Connections.NewRabbitMqTopic("goTopic", "topic.one.test1")
	two := Connections.NewRabbitMqTopic("goTopic", "topic.two.test2")

	for i := 0; i <= 10; i++ {
		one.PublishTopic("hello topic one!" + strconv.Itoa(i))
		two.PublishTopic("hello topic two" + strconv.Itoa(i))

		time.Sleep(1 * time.Second)
		fmt.Println(i)
	}

}
