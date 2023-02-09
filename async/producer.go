package async

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)
//sync 异步发送 producer可以不用管broker的返回结果就可以直接发送下一条消息
//producer直接调用回调函数来检测消息发送是否成功
//consumer接收消息还是一样的
func asyncProducer() {
	pro, err := rocketmq.NewProducer(
		producer.WithGroupName("test_producer"),
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),
		producer.WithRetry(2),
	)
	if err != nil {
		fmt.Println("producer initial error ! ", err)
		os.Exit(-1)
	}
	if pro.Start() != nil {
		fmt.Println("producer start error ")
		os.Exit(-1)
	}
	//使用waitgroup来强制go进程输出下列的打印信息
	//在具体使用场景中可以直接异步发送，不用waitgroup
	waitgroup := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		waitgroup.Add(1)
		r := pro.SendAsync(context.Background(), func(ctx context.Context, result *primitive.SendResult, err error) {
			//回调函数，检验消息发送状态
			if err != nil {
				fmt.Println("hook function get error : ", err)
			} else {
				fmt.Println(i, "st message async send success , status:", result.Status)
			}
			waitgroup.Done()
		}, &primitive.Message{
			Topic: "test",
			Body:  []byte("this is test message : " + strconv.Itoa(i)),
		})
		if r != nil {
			fmt.Println("Send async error : ", r)
		}
	}
	waitgroup.Wait()
	if pro.Shutdown() != nil {
		fmt.Println("producer shutdown error")
	}
}
