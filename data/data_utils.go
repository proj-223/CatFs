package data

import (
	"bufio"
	proc "github.com/proj-223/CatFs/protocols"
	"os"
)

// go routine to receive data
func (self *DataServer) writeBlockToDisk(data chan []byte, block *proc.CatBlock) {
	// TODO get file name
	filename := "/tmp/catfs-test/" + block.ID
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

func (self *DataServer) cleanLease(lease *proc.CatLease) {
	if _, ok := self.pipelineMap[lease.ID]; ok {
		delete(self.pipelineMap, lease.ID)
	}
	self.blockServer.StopTransaction(lease.ID)
}
