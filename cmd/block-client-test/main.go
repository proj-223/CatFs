package main

import (
	"github.com/proj-223/CatFs/config"
	"github.com/proj-223/CatFs/utils"
	"time"
)

func main() {
	id := "AA33016C-B0C8-48E8-8238-5E06B9EB27D8"
	b := utils.NewBlockClient("localhost", config.DefaultBlockServerConfig)
	c := make(chan []byte)
	go b.SendBlock(c, id)
	c <- []byte("fuck fuck fuck fuck fuck fuck")
	close(c)
	time.Sleep(time.Second * 100)
}