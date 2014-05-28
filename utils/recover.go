package utils

import (
	"fmt"
	"log"
)

func DummyRecover() {
	if x := recover(); x != nil {
		err := fmt.Errorf("%v", x)
		log.Println(err.Error())
	}
}
