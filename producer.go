package rocketmqtest

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)
func RocketMQProducer() {
	p, r := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),//这里添加nameserver的地址
		producer.WithRetry(2),
	)
	if r != nil {
		fmt.Println("initialize producer failure error :", r)
	}
	//
	err := p.Start()
	if err != nil {
		fmt.Println("Producer start error : ", err)
		return
	}
	//指定消息发送的topic
	topic := "test"
	//模拟发送10个字符消息
	for i := 0; i < 10; i++ {
		msg := &primitive.Message{
			Topic: topic,
			Body:  []byte("Hello RocketMQ Go Client! times : " + strconv.Itoa(i)),
		}
		//使用sync发送--同步发送
		res, err := p.SendSync(context.Background(), msg)
		if err != nil {
			fmt.Printf("send message error: %s\n", err)
		} else {
			fmt.Printf("send message success: result=%s\n", res.String())
		}
	}
	e := p.Shutdown()
	if e != nil {
		fmt.Println("RocketMQ Producer shutdown error : ", e)
	}
}
