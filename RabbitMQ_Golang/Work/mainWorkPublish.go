package main

import (
	"Rabbitmq/Connections"
	"fmt"
	"strconv"
	"time"
)

/*
work 模式，与 simple 相似，不同之处在于，work 模式时多个消费者共享同一个队列中的消息，解决生产力大于消费力，起到负载均衡作用‘
	1. 声明队列
	2. 发送消息
*/

func main() {
	rabbitmq := Connections.NewRabbitMqSimple("goWork")
	for i := 0; i <= 100; i++ {
		rabbitmq.PublishSimple("Hello go work" + strconv.Itoa(i))
		time.Sleep(1 * time.Second)
		fmt.Println(i)
	}

}
