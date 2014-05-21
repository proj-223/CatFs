package master

import (
	proc "github.com/proj-223/CatFs/protocols"
)

type Master struct {
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
func (self *Master) Listdir(param *proc.ListDirParam, files *proc.CatFileStatus) error {
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

// Init the Master Server
func (self *Master) Init() error {
	panic("to do")
}
