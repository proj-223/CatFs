package main

import (
	"flag"
	"github.com/proj-223/CatFs/data"
	"log"
	"strconv"
)

func main() {
	flag.Parse()
	args := flag.Args()
	i, err := strconv.Atoi(args[0])
	if err != nil {
		log.Fatal(err)
	}
	data.Serve(i)
}
