package protocols

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
)

func DummyRecover() {
	if x := recover(); x != nil {
		err := fmt.Errorf("%v", x)
		log.Println(err.Error())
	}
}

func ToBytes(s interface{}) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(s)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func FromBytes(b []byte, s interface{}) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(s)
	if err != nil {
		return err
	}
	return nil
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
