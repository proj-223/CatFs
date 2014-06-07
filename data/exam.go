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

func (self *DataServer) registerDataServer() error {
	blockDir := self.blockDir()
	var s syscall.Statfs_t
	err := syscall.Statfs(blockDir, &s)
	if err != nil {
		return err
	}
	blockReports, dataSize := self.examBlocks()
	serverStatus := &proc.DataServerStatus{
		Location:     self.location,
		AvaiableSize: s.Bavail * uint64(s.Bsize),
		TotalSize:    s.Blocks * uint64(s.Bsize),
		DataSize:     dataSize,
		BlockReports: blockReports,
	}
	registerParam := &proc.RegisterDataParam{
		Status: serverStatus,
	}
	master := self.pool.MasterServer()
	var succ bool
	err = master.RegisterDataServer(registerParam, &succ)
	if err == nil && !succ {
		err = ErrOperationFailed
	}
	return err
}

func (self *DataServer) examServer(done chan<- error) {
	err := self.registerDataServer()
	if err != nil {
		done <- err
		return
	}
	log.Printf("Server Registered to master")
	c := time.Tick(HEARTBEAT_TICK)
	for _ = range c {
		go self.examServerRoutine()
	}
}

func (self *DataServer) examServerRoutine() {
	blockDir := self.blockDir()
	var s syscall.Statfs_t
	err := syscall.Statfs(blockDir, &s)
	if err != nil {
		log.Println(err.Error())
		return
	}

	blockReports, dataSize := self.examBlocks()
	serverStatus := &proc.DataServerStatus{
		Location:     self.location,
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
	for _, command := range resp.Command {
		self.commands <- command
	}
}

func (self *DataServer) examBlocks() (map[string]*proc.DataBlockReport, uint64) {
	blockDir := self.blockDir()
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
