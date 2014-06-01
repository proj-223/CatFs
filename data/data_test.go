package data_test

import (
	"bufio"
	"github.com/proj-223/CatFs/data"
	proc "github.com/proj-223/CatFs/protocols"
	"os"
	"runtime/debug"
	"testing"
	"time"
)

func ne(e error, t *testing.T) {
	if e != nil {
		debug.PrintStack()
		t.Fatal(e)
	}
}

func er(e error, t *testing.T) {
	if e == nil {
		debug.PrintStack()
		t.Fatal()
	}
}

func as(cond bool, t *testing.T) {
	if !cond {
		debug.PrintStack()
		t.Fatal()
	}
}

func TestData(t *testing.T) {
	go data.Serve(0)
	time.Sleep(time.Second)
	block := &proc.CatBlock{
		ID:        "111",
		Locations: []proc.BlockLocation{0},
	}
	testSendBlock(block, t)
	testGetBlock(block, t)
	os.Remove(getFilename(block.ID))
}

func testGetBlock(block *proc.CatBlock, t *testing.T) {
	gbp := &proc.GetBlockParam{
		Block: block,
	}
	ds := proc.DataServer(0)
	bs := gbp.Block.Locations[0].BlockClient(proc.DefaultClientPool)
	var lease proc.CatLease
	ds.GetBlock(gbp, &lease)
	t.Logf("Get lease %s\n", lease.ID)
	c := make(chan []byte)
	go bs.GetBlock(c, lease.ID)
	for {
		b, ok := <-c
		if !ok {
			break
		}
		as("I am a test string\nI am the seond test string\n" == string(b), t)
	}
}

func testSendBlock(block *proc.CatBlock, t *testing.T) {
	pbp := &proc.PrepareBlockParam{
		Block: block,
	}
	ds := proc.DataServer(0)
	bs := pbp.Block.Locations[0].BlockClient(proc.DefaultClientPool)
	var lease proc.CatLease
	ds.PrepareSendBlock(pbp, &lease)
	t.Logf("Get lease %s\n", lease.ID)

	c := make(chan []byte)
	go bs.SendBlock(c, lease.ID)
	go func() {
		c <- []byte("I am a test string\n")
		c <- []byte("I am the seond test string\n")
		close(c)
	}()
	sbp := &proc.SendingBlockParam{
		Lease: &lease,
	}
	var succ bool
	ds.SendingBlock(sbp, &succ)
	as(succ, t)
	t.Logf("sending end\n")

	id := pbp.Block.ID
	filename := getFilename(id)
	pathExists(filename)
	fi, err := os.Open(filename)
	ne(err, t)
	r := bufio.NewReader(fi)
	line, _, err := r.ReadLine()
	ne(err, t)
	as("I am a test string" == string(line), t)
	line, _, err = r.ReadLine()
	ne(err, t)
	as("I am the seond test string" == string(line), t)
	fi.Close()
}

func getFilename(id string) string {
	return "/tmp/catfs-test/" + id
}

// exists returns whether the given file or directory exists or not
func pathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}
