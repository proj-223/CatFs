package main

import (
	"flag"
  "fmt"
)

func main() {
	flag.Parse()
	args := flag.Args()
  if len(args) == 0 {
    fmt.Println("No engouht arguments")
    return
  }
  switch args[0] {
  case "bandwidth":
    bandWidthBench(args[1:])
  default:
    fmt.Println("Benchmark not support")
  }
}
