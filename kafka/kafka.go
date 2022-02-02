package kafka

import (
	"fmt"
	"log_collector/logtransfer/es"
	"time"

	"github.com/Shopify/sarama"
)

type logData struct {
	topic string
	data  string
}

type KafkaConsumer struct {
	consumer sarama.Consumer
	topic    string
}

var (
	Addrs       []string
	producer    sarama.SyncProducer
	logDataChan chan *logData
)

func Init(addrs []string, maxSize int) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	Addrs = addrs
	producer, err = sarama.NewSyncProducer(addrs, config)
	if err != nil {
		fmt.Println("Producer closed , err:", err)
		return
	}
	logDataChan = make(chan *logData, maxSize)
	go sendToKafka()
	return
}

func SendToChan(topic, data string) {
	dat := &logData{
		topic: topic,
		data:  data,
	}
	logDataChan <- dat
}

func sendToKafka() {
	for {
		select {
		case logData := <-logDataChan:
			msg := &sarama.ProducerMessage{}
			msg.Topic = logData.topic
			msg.Value = sarama.StringEncoder(logData.data)
			_, _, err := producer.SendMessage(msg)
			if err != nil {
				fmt.Println("send msg failed , err:", err)
				return
			}
		default:
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func NewConsumer(topic string) (c *KafkaConsumer) {
	consumer, err := sarama.NewConsumer(Addrs, nil)
	if err != nil {
		fmt.Println("fail to start consumer, err:", err)
		return
	}
	c = &KafkaConsumer{
		consumer: consumer,
		topic:    topic,
	}
	go c.consume()
	return
}

func (C *KafkaConsumer) consume() (err error) {
	consumer := C.consumer
	partitionList, err := consumer.Partitions(C.topic)
	if err != nil {
		fmt.Println("fail to get list of partition , err:", err)
		return
	}
	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(C.topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("fail to start consumer for partition %d , err:%v", partition, err)
			return err
		}
		defer pc.AsyncClose()
		for msg := range pc.Messages() {
			ld := &es.LogData{
				Topic: C.topic,
				Data:  string(msg.Value),
			}
			es.SendToESChan(ld)
		}
	}
	return
}
