package delay

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func Delaypro() {
	pro, _ := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),
		producer.WithGroupName("test_producer"),
		producer.WithRetry(2),
	)
	if pro.Start() != nil {
		fmt.Println("producer start error !")
	}
	for i := 0; i < 10; i++ {
		msg := primitive.NewMessage("test1", []byte("测试"+strconv.Itoa(i)))
		//这里设置了消息接收的延迟时间，使用delay level来描述，
		//WithDelayTimeLevel set message delay time to consume. reference delay level definition: 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h delay level starts from 1. for example, if we set param level=1, then the delay time is 1s.
		msg.WithDelayTimeLevel(3)
		_, e := pro.SendSync(context.Background(), msg)
		if e != nil {
			fmt.Println("producer send error:", e)
		}
	}
	if pro.Shutdown() != nil {
		fmt.Println("producer shutdown error !")
	}
}
