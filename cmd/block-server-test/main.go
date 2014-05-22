package main

import (
	"github.com/proj-223/CatFs/config"
	"github.com/proj-223/CatFs/utils"
)

func main() {
	id := "AA33016C-B0C8-48E8-8238-5E06B9EB27D8"
	b := utils.NewBlockServer(config.DefaultBlockServerConfig)
	c := make(chan []byte)
	b.StartTransaction(id, c)
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
