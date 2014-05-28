package master

import (
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Master struct {
	conf *config.MachineConfig
}

// Get location of the block of the specified file within the specified range
func (self *Master) GetBlockLocation(query *proc.BlockQueryParam, blocks *proc.GetBlocksLocationParam) error {
	panic("to do")
}

// Create a file in a given path
func (self *Master) Create(param *proc.CreateFileParam, response *proc.OpenFileResponse) error {
	panic("to do")
}

// Open a file to add block
func (self *Master) Open(param *proc.OpenFileParam, response *proc.OpenFileResponse) error {
	panic("to do")
}

// Drop a block
func (self *Master) AbandonBlock(param *proc.AbandonBlockParam, succ *bool) error {
	panic("to do")
}

// Add a block to a specific path (file)
func (self *Master) AddBlock(param *proc.AddBlockParam, block *proc.CatBlock) error {
	panic("to do")
}

// Complete an operation,
// delete the lease (lock)
func (self *Master) Close(param *proc.CloseParam, succ *bool) error {
	panic("to do")
}

// Rename
func (self *Master) Rename(param *proc.RenameParam, succ *bool) error {
	panic("to do")
}

// Delete a file
func (self *Master) Delete(param *proc.DeleteParam, succ *bool) error {
	panic("to do")
}

// Create a dir
func (self *Master) Mkdirs(param *proc.MkdirParam, succ *bool) error {
	panic("to do")
}

// List dir
func (self *Master) Listdir(param *proc.ListDirParam, response *proc.ListdirResponse) error {
	panic("to do")
}

// Renew a lease
func (self *Master) RenewLease(oldLease *proc.CatFileLease, newLease *proc.CatFileLease) error {
	panic("to do")
}

// File info
func (self *Master) GetFileInfo(path string, filestatus *proc.CatFileStatus) error {
	panic("to do")
}

// Register a data server
func (self *Master) RegisterDataServer(param *proc.RegisterDataParam, succ *bool) error {
	panic("to do")
}

// Send heartbeat to master
func (self *Master) SendHeartbeat(param *proc.HeartbeatParam, rep *proc.HeartbeatResponse) error {
	panic("to do")
}

// Send blockreport to master
func (self *Master) BlockReport(param *proc.BlockReportParam, rep *proc.BlockReportResponse) error {
	panic("to do")
}

// go routine to init the data rpc server
func (self *Master) initRPCServer(done chan error) {
	server := rpc.NewServer()
	server.Register(proc.MasterProtocol(self))
	l, err := net.Listen("tcp", self.conf.MasterAddr())
	if err != nil {
		done <- err
		return
	}
	log.Printf(START_MSG, self.conf.MasterAddr())
	err = http.Serve(l, server)
	done <- err
}
