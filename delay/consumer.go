package delay

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func Delaycon() {
	con, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName("test_consumer"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),
		consumer.WithRetry(2),
	)
	if con.Subscribe("test1",
		consumer.MessageSelector{},
		func(ctx context.Context, me ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, m := range me {
				//接收消息的同时查看消息的产生日期
				//可以和接收日期做对比
				fmt.Println(m, "time : ", m.BornTimestamp)
			}
			return consumer.ConsumeSuccess, nil
		}) != nil {
		fmt.Println("consuemr subscirbe error ! ")
		os.Exit(-1)
	}
	if con.Start() != nil {
		fmt.Println("consuemr start error ! ")
		os.Exit(-1)
	}
	time.Sleep(time.Hour)
	con.Shutdown()
}
