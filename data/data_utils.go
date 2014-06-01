package data

import (
	"bufio"
	proc "github.com/proj-223/CatFs/protocols"
	"io"
	"os"
)

const (
	DEFAULT_BLOCK_BUFFER = 1024
)

func (self *DataServer) blockFilename(block *proc.CatBlock) string {
	// TODO get file name
	return "/tmp/catfs-test/" + block.ID
}

// go routine to receive data
func (self *DataServer) writeBlockToDisk(data <-chan []byte, block *proc.CatBlock) {
	filename := self.blockFilename(block)
	fi, err := os.Open(filename)
	if err != nil {
		// IF error, TODO sth
		return
	}
	defer fi.Close()
	writer := bufio.NewWriter(fi)
	for {
		b, ok := <-data
		if !ok {
			// finish writing
			writer.Flush()
			break
		}
		if b == nil {
			// TODO failed writing
			break
		}
		writer.Write(b)
	}
}

func (self *DataServer) readBlockFromDisk(data chan<- []byte, block *proc.CatBlock) {
	filename := self.blockFilename(block)
	fi, err := os.Open(filename)
	if err != nil {
		// TODO
		panic(err)
	}
	defer fi.Close()
	r := bufio.NewReader(fi)
	buf := make([]byte, DEFAULT_BLOCK_BUFFER)
	for {
		// read a chunk
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			// TODO
			panic(err)
		}
		if n == 0 {
			break
		}
		data <- buf
	}
}

func (self *DataServer) cleanLease(lease *proc.CatLease) {
	if _, ok := self.pipelineMap[lease.ID]; ok {
		delete(self.pipelineMap, lease.ID)
	}
	if _, ok := self.leaseMap[lease.ID]; ok {
		delete(self.leaseMap, lease.ID)
	}
	self.blockServer.StopTransaction(lease.ID)
}
