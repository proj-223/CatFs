package main

import (
	"flag"
	"fmt"
	"github.com/proj-223/CatFs/client"
	"github.com/proj-223/CatFs/config"
)

func main() {
	config.LoadConfig("/tmp/catfs.json")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("No engouht arguments")
		return
	}
	switch args[0] {
	case "bandwidth-write":
		bandWidthBenchWrite(args[1:])
	case "bandwidth-read":
		bandWidthBenchRead(args[1:])
	case "op-mkdir":
		opBenchMkdir(args[1:], func(c *client.CatClient, fnn string) {
			c.Mkdir(fnn, 0)
		})

	case "op-delete":
		opBenchMkdir(args[1:], func(c *client.CatClient, fnn string) {
			c.Mkdir(fnn, 0)
			c.Remove(fnn)
		})
	default:
		fmt.Println("Benchmark not support")
	}
}
