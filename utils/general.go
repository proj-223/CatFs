package utils

import (
	"strconv"
	"time"
)

func GetTimestamp() string {
	return strconv.Itoa(int(time.Now().UnixNano()))
}

func NewTimeout(seconds int) chan bool {
	timeout := make(chan bool, 1)
	go func(seconds int) {
		time.Sleep(time.Duration(seconds) * time.Second)
		timeout <- true
	}(seconds)
	return timeout
}
