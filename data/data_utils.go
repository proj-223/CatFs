package data

import (
	"bufio"
	proc "github.com/proj-223/CatFs/protocols"
	"io"
	"os"
)

const (
	DEFAULT_BLOCK_BUFFER = 1024
	DEFAULT_FILE_PERM    = 0664
)

func (self *DataServer) blockFilename(block *proc.CatBlock) string {
	return self.blockFilenameFromID(block.ID)
}

func (self *DataServer) blockFilenameFromID(blockID string) string {
	path := self.conf.BlockPath(self.index)
	return path + "/" + blockID
}

// go routine to receive data
func (self *DataServer) writeBlockToDisk(data <-chan []byte, block *proc.CatBlock) {
	filename := self.blockFilename(block)
	fi, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, DEFAULT_FILE_PERM)
	if err != nil {
		panic(err)
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
			close(data)
			break
		}
		data <- buf[:n]
	}
}

func (self *DataServer) initBlockDir() error {
	path := self.conf.BlockPath(self.index)
	finfo, err := os.Stat(path)
	if err == nil && finfo.IsDir() {
		return nil
	}
	if err == nil && !finfo.IsDir() {
		return ErrInvalidPath
	}
	// create dir
	return os.MkdirAll(path, 0775)
}

func (self *DataServer) registerLeaseListener() {
	self.leaseManager.OnRemoveLease(func(lease *proc.CatLease) {
		if _, ok := self.pipelineMap[lease.ID]; ok {
			delete(self.pipelineMap, lease.ID)
		}
		if _, ok := self.leaseMap[lease.ID]; ok {
			delete(self.leaseMap, lease.ID)
		}
	})
}
