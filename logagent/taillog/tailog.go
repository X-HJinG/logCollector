package taillog

import (
	"context"
	"fmt"
	"log_collector/kafka"

	"github.com/hpcloud/tail"
)

type TailTask struct {
	path       string
	topic      string
	instance   *tail.Tail
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewTailTask(path, topic string) (tailTask *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailTask = &TailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	tailTask.init()
	return
}

func (T *TailTask) init() {
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	var err error
	T.instance, err = tail.TailFile(T.path, config)
	if err != nil {
		fmt.Println("tail file failed , err:", err)
		return
	}
	go T.run()
}

func (T *TailTask) run() {
	for {
		select {
		case line := <-T.instance.Lines:
			kafka.SendToChan(T.topic, line.Text)
		case <-T.ctx.Done():
			fmt.Printf("tailTask:%s_%s end...\n", T.path, T.topic)
			return
		}
	}
}
