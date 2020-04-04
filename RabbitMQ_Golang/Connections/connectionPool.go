package Connections

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

const MQURL = "amqp://MincoX:mincoroot@49.232.19.51:5672/go"

type RabbitMq struct {
	conn    *amqp.Connection
	channel *amqp.Channel

	// 队列名称
	QueueName string
	// 交换机
	Exchange string
	// key 用于其他模式
	Key string
	// 连接信息
	MqUrl string
}

// 关闭 RabbitMq 的连接
func (r *RabbitMq) Destroy() {
	r.channel.Close()
	r.conn.Close()
}

// 错误处理函数
func (r *RabbitMq) failOnError(err error, message string) {
	if err != nil {
		fmt.Printf("%s:%s", message, err)
		log.Fatalf("%s:%s", message, err)
	}
}

// 话题模式发送消息
func (r *RabbitMq) PublishTopic(message string) {
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare an exchange!")

	// 发送消息
	err = r.channel.Publish(
		r.Exchange,
		// 路由模式下发送消息必须要设置 key
		r.Key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

// 话题模式的 key, 通过 . 号分隔每一个单词
// 话题模式下的消费
func (r *RabbitMq) ReceiveTopic() {

	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)

	r.failOnError(err, "Failed to declare an exchange!")

	q, err := r.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare a queue!")

	// 绑定队列到交换机上
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.Exchange,
		false,
		nil,
	)

	// 消费消息
	messages, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	forever := make(chan bool)

	go func() {
		for d := range messages {
			log.Printf("Receive a message: %s", d.Body)
		}
	}()

	fmt.Println("CTRL+C, 退出！")

	<-forever

}

// simple 模式下的生产(发送方)
func (r *RabbitMq) PublishSimple(message string) {
	// 1. 申请队列，如果队列不存在会自动创建，如果存在则跳过创建，保证队列存在，消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		// 控制消息是否持久化
		false,
		// 是否为自动删除（当最后一个消费者断开连接以后，是否将消息重队列中删除）
		false,
		// 排他性，若为 true 其他用户无法访问，自己可见
		false,
		// 是否阻塞（发送消息后等待服务器是否有响应）
		false,
		// 可变参数
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}

	// 2. 发送消息到队列中
	r.channel.Publish(
		// '', 使用默认的交换机，类型为 direct
		r.Exchange,
		r.QueueName,
		// 如果为 true, 当根据 exchange 和 routing key 匹配不到队列时，会把发送的消息返回给发送者
		false,
		// 如果为 true, 当 exchange 发送消息到队列后发现队列上没有绑定消费者，则会把消息发给发送者
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

// simple 模式下的消费者
func (r *RabbitMq) ConsumeSimple() {
	// 1. 申请队列，如果队列不存在会自动创建，如果存在则跳过创建，保证队列存在，消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		// 控制消息是否持久化
		false,
		// 是否为自动删除（当最后一个消费者断开连接以后，是否将消息重队列中删除）
		false,
		// 排他性，若为 true 其他用户无法访问，自己可见
		false,
		// 是否阻塞（发送消息后等待服务器是否有响应）
		false,
		// 可变参数
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}
	// 2. 接收消息
	msgs, err := r.channel.Consume(
		r.QueueName,
		// 用来区分多个消费者
		"",
		// 是否自动应答
		true,
		// 是否具有排他性
		false,
		// 如果设置为 true， 表示不能将同一个 connections 中发送的消息传递给这个 connection 中的消费者
		false,
		// 队列消费是否阻塞
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}

	forever := make(chan bool)
	// 启用协程处理消息
	go func() {
		for d := range msgs {
			// 接收到消息，要实现的逻辑
			log.Printf("Received a message: %s", d.Body)
			fmt.Println(d.Body)
		}
	}()

	// 阻塞
	log.Printf("[*] Waiting for message, To exit press CTRL+C!")
	<-forever

}

// 订阅模式的生产
func (r *RabbitMq) PublishPub(message string) {
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		// 广播类型
		"fanout",
		true,
		false,
		// 如果为 true, 表示这个 exchange 不可以被 client 用来推送消息，仅用来进行 exchange 和 exchange 之间的绑定
		false,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare an exchange!")

	err = r.channel.Publish(
		r.Exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

// 订阅模式消费端代码
func (r *RabbitMq) ReceiveSub() {
	// 1. 创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare an exchange!")

	// 2. 创建队列
	q, err := r.channel.QueueDeclare(
		// 随机生成队列名称
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare a queue!")

	// 绑定队列到 exchange 中
	err = r.channel.QueueBind(
		q.Name,
		// 在 pub/sub 模式下，这里的 key 要为空
		"",
		r.Exchange,
		false,
		nil,
	)

	// 消费消息
	message, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	forever := make(chan bool)

	go func() {

		for d := range message {
			log.Printf("Receive a message: %s", d.Body)

		}
	}()

	fmt.Println("CTRL+C, 退出！")
	<-forever
}

// 路由模式发送消息
func (r *RabbitMq) PublishRouting(message string) {
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare an exchange!")

	// 发送消息
	err = r.channel.Publish(
		r.Exchange,
		// 路由模式下发送消息必须要设置 key
		r.Key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

// 路由模式消息的接收
func (r *RabbitMq) ReceiveRouting() {
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare an exchange!")

	q, err := r.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	r.failOnError(err, "Failed to declare a queue!")

	// 绑定队列到交换机上
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.Exchange,
		false,
		nil,
	)

	// 消费消息
	messages, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	forever := make(chan bool)

	go func() {
		for d := range messages {
			log.Printf("Receive a message: %s", d.Body)
		}
	}()

	fmt.Println("CTRL+C, 退出！")

	<-forever

}

// 创建 RabbitMq 结构体实例（基础实例，在每一种模式下都会使用的实例）
func NewRabbitMq(queueName string, exchange string, key string) *RabbitMq {
	rabbitmq := &RabbitMq{QueueName: queueName, Exchange: exchange, Key: key, MqUrl: MQURL}

	return rabbitmq

}

// 创建 simple 模式下 RabbitMq 实例
// 根据不同模式下传入不同的实例参数， simple 模式下只用传入 queueName, 使用 "" 默认的交换机, 不需要使用 ""
func NewRabbitMqSimple(queueName string) *RabbitMq {
	rabbitmq := NewRabbitMq(queueName, "", "")

	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MqUrl)
	rabbitmq.failOnError(err, "创建连接错误！")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnError(err, "获取 channel 失败！")

	return rabbitmq

}

// 订阅模式 publish/subscribe 创建 RabbitMq 的实例
func NewRabbitMqPubSub(exchangeName string) *RabbitMq {
	// 创建 RabbitMq 实例
	rabbitmq := NewRabbitMq("", exchangeName, "")
	var err error
	// 获取 connection
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MqUrl)

	rabbitmq.failOnError(err, "failed to connect rabbitmq!")
	// 获取 channel
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnError(err, "failed to open a channel!")

	return rabbitmq

}

// 路由模式（routing），创建实例
func NewRabbitMqRouting(exchangeName string, routingKey string) *RabbitMq {
	rabbitmq := NewRabbitMq("", exchangeName, routingKey)
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MqUrl)

	rabbitmq.failOnError(err, "Failed to connect rabbitmq!")

	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnError(err, "Failed to open a channel!")

	return rabbitmq
}

// 话题（Topic）模式，创建实例
func NewRabbitMqTopic(exchangeName string, routingKey string) *RabbitMq {

	rabbitmq := NewRabbitMq("", exchangeName, routingKey)
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MqUrl)
	rabbitmq.failOnError(err, "Failed to connect rabbitmq!")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnError(err, "Failed to open a channel!")

	return rabbitmq
}
