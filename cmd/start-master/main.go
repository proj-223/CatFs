package main

import (
	"github.com/proj-223/CatFs/config"
	"github.com/proj-223/CatFs/master"
)

func main() {
	config.LoadConfig("/tmp/catfs.json")
	master.Serve()
}
