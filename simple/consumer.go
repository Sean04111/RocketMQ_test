package rocketmqtest

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func RocketMQConsumer() {
	sig := make(chan os.Signal)
	c, e := rocketmq.NewPushConsumer(
		//注册conusmergroup
		consumer.WithGroupName("test_consumer"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),
	)
	if e != nil {
		fmt.Println("consumer initialize failure error : ", e)
		return
	}
	//conusmer要先使用需要先subscribe某个特定的topic
	//这里指定关注topic---test
	err := c.Subscribe("test", consumer.MessageSelector{}, func(ctx context.Context, megs ...*primitive.MessageExt) (consumer.ConsumeResult, error) { //订阅的同时，定义了消息接收处理函数
		for i := range megs {
			//这里只是简单出打印这个消息，包括tag、key、body等等
			fmt.Println("Message : ", megs[i])
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Println("consumer subscibe error")
		return
	}
	r := c.Start()
	if r != nil {
		fmt.Println("client start error   ", r)
		os.Exit(-1)
	}
	<-sig
	err = c.Shutdown()
	if err != nil {
		fmt.Printf("shutdown Consumer error: %s", err.Error())
	}
}
