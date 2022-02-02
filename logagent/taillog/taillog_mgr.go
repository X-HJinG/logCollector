package taillog

import (
	"fmt"
	"log_collector/etcd"
	"log_collector/kafka"
	"time"
)

var tskMgr *tailLogMgr

type tailLogMgr struct {
	logEntry    []*etcd.LogEntry
	tskMap      map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

func Init(logEntryConf []*etcd.LogEntry) {
	tskMgr = &tailLogMgr{
		logEntry:    logEntryConf,
		tskMap:      make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry),
	}
	for _, logEntry := range logEntryConf {
		tsk := NewTailTask(logEntry.Path, logEntry.Topic)
		kafka.NewConsumer(logEntry.Topic)
		mkey := fmt.Sprintf("%s_%s", logEntry.Path, logEntry.Topic)
		tskMgr.tskMap[mkey] = tsk
	}
	go tskMgr.run()
}

func (T *tailLogMgr) run() {
	for {
		select {
		case newConf := <-T.newConfChan:
			for _, conf := range newConf {
				mkey := fmt.Sprintf("%s_%s", conf.Path, conf.Topic)
				if _, ok := T.tskMap[mkey]; ok {
					continue
				} else {
					tsk := NewTailTask(conf.Path, conf.Topic)
					kafka.NewConsumer(conf.Topic)
					T.tskMap[mkey] = tsk
				}
			}
			for _, v1 := range T.logEntry {
				isDelete := true
				for _, v2 := range newConf {
					if v2.Path == v1.Path && v2.Topic == v1.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					mkey := fmt.Sprintf("%s_%s", v1.Path, v1.Topic)
					tskMgr.tskMap[mkey].cancelFunc()
					delete(tskMgr.tskMap, mkey)
				}
			}
		default:
			time.Sleep(time.Second)
		}
	}
}

func NewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
