package main

import (
	"Rabbitmq/Connections"
	"fmt"
	"strconv"
	"time"
)

/*
订阅 direct 模式（对绑定到交换机上的队列进行 routing_key 匹配）：
	1. 声明交换机，指明模式为 direct
	2. 发送消息，指明 routing_key（此消息所要匹配的 routing_key 队列）
*/
func main() {
	one := Connections.NewRabbitMqRouting("goRouting", "one")
	two := Connections.NewRabbitMqRouting("goRouting", "two")

	for i := 0; i <= 10; i++ {
		one.PublishRouting("hello one" + strconv.Itoa(i))
		two.PublishRouting("hello two" + strconv.Itoa(i))

		time.Sleep(1 * time.Second)
		fmt.Println(i)

	}

}
