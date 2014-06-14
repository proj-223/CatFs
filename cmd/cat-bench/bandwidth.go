package main

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/proj-223/CatFs/client"
	proc "github.com/proj-223/CatFs/protocols"
	"io"
	"log"
	"strconv"
	"time"
)

func bandWidthBenchWrite(args []string) {
	worker, err := strconv.Atoi(args[0])
	if err != nil {
		log.Fatal(err)
	}
	mb, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal(err)
	}
	done := make(chan bool, worker)
	t1 := time.Now()
	for i := 0; i < worker; i++ {
		go writeOp(done, mb)
	}
	for i := 0; i < worker; i++ {
		<-done
	}
	t2 := time.Now()
	td := t2.UnixNano() - t1.UnixNano()
	println(td)
}

func writeOp(done chan bool, mb int) {
	c := client.NewCatClient()
	fi, err := c.Create(uuid.New())
	if err != nil {
		printError(err)
		return
	}
	buf := make([]byte, 1<<10)
	for i := 0; i < mb*(1<<10); i++ {
		_, err := fi.Write(buf)
		if err != nil {
			printError(err)
			return
		}
	}
	fi.Close()
	done <- true
}

func printError(err error) {
	println("Error :", err.Error())
}

func bandWidthBenchRead(args []string) {
	path := args[0]
	worker, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal(err)
	}
	mb, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatal(err)
	}
	done := make(chan bool, worker)
	t1 := time.Now()
	for i := 0; i < worker; i++ {
		go readOp(done, path, mb)
	}
	for i := 0; i < worker; i++ {
		<-done
	}
	t2 := time.Now()
	td := t2.UnixNano() - t1.UnixNano()
	println(td)
}

func readOp(done chan bool, path string, mb int) {
	c := client.NewCatClient()
	fi, err := c.Open(path, proc.OPEN_MODE_READ)
	if err != nil {
		log.Printf("Error %s", err.Error())
		done <- false
		return
	}
	buf := make([]byte, 1<<10)
	for i := 0; i < mb*(1<<10); i++ {
		_, err := fi.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error %s", err.Error())
			done <- false
			return
		}
	}
	fi.Close()
	done <- true
}
