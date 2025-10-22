// Package rpc for rpc call
package rpc

import (
	"net/http"
	"time"
)

var TaskClient *http.Client

func init() {
	InitClient()
}

//InitClient func for Init client
func InitClient() {
	tr := &http.Transport{
		MaxIdleConns:        600,
		MaxIdleConnsPerHost: 600,
		IdleConnTimeout:     10 * time.Second,
	}
	TaskClient = &http.Client{Timeout: 10 * time.Second, Transport: tr}
}

//GetTodayTime func get today timestamp
func GetTodayTime() int64 {
	var now = time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).Unix()
}
