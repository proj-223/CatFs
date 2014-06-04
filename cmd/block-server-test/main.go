package main

import (
	"github.com/proj-223/CatFs/config"
	"github.com/proj-223/CatFs/data"
)

func main() {
	id := "AA33016C-B0C8-48E8-8238-5E06B9EB27D8"
	b := data.NewBlockServer(config.DefaultBlockServerConfig)
	c := make(chan []byte)
	done := make(chan bool)
	trans := data.NewReadTransaction(id, done, c)
	b.StartTransaction(trans)
	/*
		go func() {
			c <- []byte("fuck fuck fuck fuck fuck fuck")
		}()
	*/

	go func() {
		<-done
		println("done")
	}()
	go func() {
		for {
			b, ok := <-c
			if !ok {
				break
			}
			println(string(b))
		}
	}()
	b.Serve()
}
