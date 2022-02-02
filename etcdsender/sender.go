package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type JsonData struct {
	Path  string `json:"path"`
	Topic string `json:"topic"`
}

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect to etcd failed , err:", err)
		return
	}
	fmt.Println("connect to etcd success")
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	jsonStr, err := readJson("../conf/catalog.json")
	if err != nil {
		fmt.Println("get json failed , err", err)
		return
	}
	ip := func() (ip string) {
		conn, err := net.Dial("udp", "8.8.8.8:80")
		if err != nil {
			return
		}
		defer conn.Close()
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		ip = strings.Split(localAddr.IP.String(), ":")[0]
		return
	}()
	key := fmt.Sprintf("/logagent/%s/collect", ip)
	fmt.Println(key)
	_, err = cli.Put(ctx, key, jsonStr)
	cancel()
	if err != nil {
		fmt.Println("put to etcd failed , err :", err)
		return
	}
}

func readJson(fileName string) (res string, err error) {
	jsonFile, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println("Open file failed , err : ", err)
		return
	}
	dataSet := make([]JsonData, 100)
	err = json.Unmarshal(jsonFile, &dataSet)
	if err != nil {
		fmt.Println("Json Unmarshal failed , err:", err)
		return
	}
	tmp := make([]string, 0, 20)
	for _, data := range dataSet {
		item := fmt.Sprintf(`{"path":"%s","topic":"%s"}`, data.Path, data.Topic)
		tmp = append(tmp, item)
	}
	res = "[" + strings.Join(tmp, ",") + "]"
	return
}
