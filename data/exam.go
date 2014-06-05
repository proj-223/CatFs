package data

import (
	proc "github.com/proj-223/CatFs/protocols"
	"io/ioutil"
	"log"
	"syscall"
	"time"
)

const (
	HEARTBEAT_TICK = 10 * time.Second
)

func (self *DataServer) examServer() {
	c := time.Tick(HEARTBEAT_TICK)
	for _ = range c {
		go self.examServerRoutine()
	}
}

func (self *DataServer) examServerRoutine() {
	blockDir := self.conf.BlockPath(self.index)
	var s syscall.Statfs_t
	err := syscall.Statfs(blockDir, &s)
	if err != nil {
		log.Println(err.Error())
		return
	}

	blockReports, dataSize := self.examBlocks()
	serverStatus := &proc.DataServerStatus{
		Location:     proc.ServerLocation(self.index),
		AvaiableSize: s.Bavail * uint64(s.Bsize),
		TotalSize:    s.Blocks * uint64(s.Bsize),
		DataSize:     dataSize,
		BlockReports: blockReports,
	}
	heartbeat := &proc.HeartbeatParam{
		Status: serverStatus,
	}
	master := self.pool.MasterServer()
	var resp proc.HeartbeatResponse
	master.SendHeartbeat(heartbeat, &resp)
	// TODO handle the response
	// TODO TODO TODO
}

func (self *DataServer) examBlocks() (map[string]*proc.DataBlockReport, uint64) {
	blockDir := self.conf.BlockPath(self.index)
	var dataSize uint64
	blockReports := make(map[string]*proc.DataBlockReport)
	files, _ := ioutil.ReadDir(blockDir)
	for _, file := range files {
		dataSize += uint64(file.Size())
		// TODO verify wether block is ok
		blockReport := &proc.DataBlockReport{
			ID:     file.Name(),
			Status: proc.BLOCK_OK,
		}
		blockReports[file.Name()] = blockReport
	}
	return blockReports, dataSize
}
