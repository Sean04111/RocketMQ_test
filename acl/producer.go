package acl

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func AclProducer() {
	sig := make(chan os.Signal)
	pro, err := rocketmq.NewProducer(
		producer.WithGroupName("test_producer"),
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"119.3.47.145:9876"})),
		producer.WithRetry(2),
		//在这里设置目标consumer的key
		producer.WithCredentials(primitive.Credentials{
			AccessKey: "acl",
			SecretKey: "666",
		}),
	)
	if err != nil {
		fmt.Println("Producer initialize error : ", err)
		return
	}
	e := pro.Start()
	if e != nil {
		fmt.Println("producer start  error : ", e)
	}
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * 2)
		res, r := pro.SendSync(context.Background(), &primitive.Message{
			Topic: "test",
			Body: []byte(`{
				"name":"Sean",
				"body":"hi",
			}
			`),
		})
		if r != nil {
			fmt.Println(i, "st message send error : ", r)
			break
		} else {
			fmt.Println(i, "st message send success  result : ", res.String())
		}
	}
	<-sig
	error := pro.Shutdown()
	if error != nil {
		fmt.Println("producer shotdown error : ", error)
	}
}
