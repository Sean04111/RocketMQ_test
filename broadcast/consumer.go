package broadcast

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)
//Broadcasting 模式会使得consumer集群中的所有consumer都会消费一次指定topic的所有消息
//所以要注意，使用Broadcasting模式中，在dashboad中message的状态会是Not_Consume_Yet!
func BroaConsumer() {
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),
		consumer.WithGroupName("test_consumer"),
		consumer.WithConsumerModel(consumer.BroadCasting),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
	)
	if c.Subscribe("test1",
		consumer.MessageSelector{},
		func(ctx context.Context, me ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for i := range me {
				fmt.Println(string(me[i].Body))
			}
			return consumer.ConsumeSuccess, nil
		},
	) != nil {
		fmt.Println("subscribe error")
		os.Exit(-1)
	}
	if c.Start() != nil {
		fmt.Println("consumer start error ")
		os.Exit(-1)
	}
	time.Sleep(time.Hour)
	c.Shutdown()
}
