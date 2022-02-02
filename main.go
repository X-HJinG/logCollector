package main

import (
	"fmt"
	"log_collector/conf"
	"log_collector/etcd"
	"log_collector/kafka"
	"log_collector/logagent/taillog"
	"log_collector/logtransfer/es"
	"log_collector/utils"
	"sync"
	"time"

	"gopkg.in/ini.v1"
)

var (
	cfg = new(conf.AppConf)
	wg  sync.WaitGroup
)

func main() {
	wg.Add(1)
	err := ini.MapTo(cfg, "../conf/config.ini")
	if err != nil {
		fmt.Println("Get config failed , err:", err)
		return
	}
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.ChanMaxSize)
	if err != nil {
		fmt.Println("Init Kafka failed,err:", err)
		return
	}
	fmt.Println("Init Kafka success!")
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Println("Init etcd failed , err:", err)
		return
	}
	fmt.Println("Init etcd success!")
	err = es.Init(cfg.EsConf.Address)
	if err != nil {
		fmt.Println("Init etc failed , err:", err)
		return
	}
	ip, err := utils.GetOutboundIP()
	if err != nil {
		panic(err)
	}
	etcdConfKey := fmt.Sprintf(cfg.EtcdConf.Key, ip)
	LogEntrys, err := etcd.GetConf(etcdConfKey)
	if err != nil {
		fmt.Println("Get conf etcd failed , err:", err)
		return
	}
	fmt.Println("Get conf etcd success!")
	taillog.Init(LogEntrys)
	newConfChan := taillog.NewConfChan()
	go etcd.WatchConf(etcdConfKey, newConfChan)
	wg.Wait()
}
