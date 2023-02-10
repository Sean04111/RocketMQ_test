package interceptor

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func InterPro() {
	p, _ := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),
		producer.WithRetry(2),
		//注册interceptor
		producer.WithInterceptor(ProFirstInterceptor()),
	)
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}
	for i := 0; i < 10; i++ {
		_, err := p.SendSync(context.Background(), primitive.NewMessage("test1",
			[]byte("interceptor 测试!")))

		if err != nil {
			fmt.Printf("send message error: %s\n", err)
		}
	}
	err = p.Shutdown()
	if err != nil {
		fmt.Printf("shutdown producer error: %s", err.Error())
	}
}

//interceptor函数
func ProFirstInterceptor() primitive.Interceptor {
	return func(ctx context.Context, req, reply interface{}, next primitive.Invoker) error {
		//next就是发送消息的函数
		//在producer中，next接收ctx、req、reply三个参数
		fmt.Println("interceptor 调用前")
		e := next(ctx, req, reply)
		if e != nil {
			fmt.Println("call failure")
		}
		fmt.Println("interceptor 调用结束")
		return e
	}
}
