package interceptor

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func InterCon() {
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName("test_consumer"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),
		consumer.WithInterceptor(ConFistInterceptor()))
	err := c.Subscribe("test1", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		fmt.Printf("消息: %v \n", msgs)
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Println(err.Error())
	}
	// Note: start after subscribe
	err = c.Start()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	time.Sleep(time.Hour)
	err = c.Shutdown()
	if err != nil {
		fmt.Printf("Shutdown Consumer error: %s", err.Error())
	}
}
func ConFistInterceptor() primitive.Interceptor {
	return func(ctx context.Context, req, reply interface{}, next primitive.Invoker) error {
		fmt.Println("interceptor 调用前")
		//consumer interceptor的next接收的参数就是ctx、msg、reply和producer不一样
		msgs := req.([]*primitive.MessageExt)//这里要注意将req定成具体消息类型
		e := next(ctx, msgs, reply)
		if e != nil {
			fmt.Println("call failure")
		}
		fmt.Println("interceptor 调用结束")
		return e
	}
}
