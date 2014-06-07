package main

import (
	"github.com/proj-223/CatFs/config"
	"github.com/proj-223/CatFs/protocols/pool"
	"time"
)

func main() {
	id := "AA33016C-B0C8-48E8-8238-5E06B9EB27D8"
	b := pool.NewBlockClient(0, config.DefaultMachineConfig)
	c := make(chan []byte)
	go b.SendBlock(c, id)
	go func() {
		c <- []byte("fuck fuck fuck fuck fuck fuck")
		close(c)
	}()
	// go b.GetBlock(c, id)
	/*
		for {
			b, ok := <-c
			if !ok {
				break
			}
			println(string(b))
		}
	*/
	time.Sleep(time.Second * 100)
}
