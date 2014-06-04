package data

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

func closeByteChan(c chan<- []byte) {
	defer DummyRecover()
	close(c)
}

func doneChan(done chan bool) {
	if done != nil {
		done <- true
	}
}
