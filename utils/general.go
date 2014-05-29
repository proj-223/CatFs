package utils

import (
	"strconv"
	"time"
)

func GetTimestamp() string {
	return strconv.Itoa(int(time.Now().UnixNano()))
}
