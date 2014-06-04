package pool

import (
	"bytes"
	"encoding/gob"
)

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
