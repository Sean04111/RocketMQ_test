package acl

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)
//acl 就是在producer和consumer之间加上一层验证关系，限定拥有同样的Credential的consumer才能消费
func AclConsumer() {
	sig := make(chan os.Signal)
	con, e := rocketmq.NewPushConsumer(
		consumer.WithGroupName("test_consumer"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),
		//只需要在这里添加一个Credential，就能与producer进行验证
		consumer.WithCredentials(primitive.Credentials{
			AccessKey: "acl",
			SecretKey: "666",
		}),
	)
	if e != nil {
		fmt.Println("conusmer initialize error : ", e)
		return
	}

	con.Subscribe("test", consumer.MessageSelector{}, func(ctx context.Context, me ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for i := range me {
			fmt.Println(i, "st message : ", string(me[i].Body))
		}
		return consumer.ConsumeSuccess, nil
	})
	er := con.Start()
	if er != nil {
		fmt.Println("consumer start error : ", er)
		return
	}
	<-sig
	if con.Shutdown() != nil {
		fmt.Println("consumer shutdown error")
		return
	}
}
