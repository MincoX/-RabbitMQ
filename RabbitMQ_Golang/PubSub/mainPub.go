package main

import (
	"Rabbitmq/Connections"
	"fmt"
	"strconv"
	"time"
)

/*
订阅 fanout 模式生产者（向所有绑定到此交换机上的队列发送消息）：
	1. 声明交换机
	2. 发送信息
*/

func main() {
	rabbitmq := Connections.NewRabbitMqPubSub("goPubSub")

	for i := 0; i < 100; i++ {
		rabbitmq.PublishPub("订阅模式生产第" + strconv.Itoa(i) + "条数据")
		time.Sleep(1 * time.Second)
		fmt.Println(i)
	}
}
