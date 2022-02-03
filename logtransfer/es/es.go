package es

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/olivere/elastic"
)

type LogData struct {
	Topic string
	Data  string
}

var (
	client *elastic.Client
	ch     = make(chan *LogData, 100000)
)

func Init(address string) (err error) {
	if !strings.HasPrefix(address, "http://") {
		address = "http://" + address
	}
	client, err = elastic.NewClient(elastic.SetURL(address))
	if err != nil {
		fmt.Println("fail to connect es , err:", err)
		return
	}
	fmt.Println("connect to es success!")
	go sendToES()
	return
}

func SendToESChan(msg *LogData) {
	ch <- msg
}

func sendToES() {
	for {
		select {
		case msg := <-ch:
			put, err := client.Index().Index(msg.Topic).Type("/test").BodyJson(msg).Do(context.Background())
			if err != nil {
				fmt.Println("send to es failed , err:", err)
				return
			}
			fmt.Printf("Indexed : %s to index : %s, type : %s\n", put.Id, put.Index, put.Type)
		default:
			time.Sleep(time.Second)
		}
	}

}
