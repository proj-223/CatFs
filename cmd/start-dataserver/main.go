package main

import (
	"flag"
	"github.com/proj-223/CatFs/config"
	"github.com/proj-223/CatFs/data"
	"log"
	"strconv"
)

func main() {
	flag.Parse()
	args := flag.Args()
	config.LoadConfig("/tmp/catfs.json")
	i, err := strconv.Atoi(args[0])
	if err != nil {
		log.Fatal(err)
	}
	for _, arg := range args[1:] {
		ii, err := strconv.Atoi(arg)
		if err != nil {
			log.Fatal(err)
		}
		go func(index int) {
			data.Serve(index)
		}(ii)
	}
	data.Serve(i)
}
