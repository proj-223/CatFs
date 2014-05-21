package data

import (
	proc "github.com/proj-223/CatFs/protocols"
)

type DataServer struct {
}

// Prepare send a block to datanode
func (self *DataServer) PrepareSendBlock(param *proc.PrepareBlockParam, lease *proc.CatLease) error {
	panic("to do")
}

// Wait util blocks reach destination
// The block will be sent by a pipeline
func (self *DataServer) SendingBlock(param *proc.SendingBlockParam, succ *bool) error {
	panic("to do")
}

// Get the block from data server
// Will start an tcp connect to request block
func (self *DataServer) GetBlock(param *proc.GetBlockParam, lease *proc.CatLease) error {
	panic("to do")
}

func (self *DataServer) Init() error {
	panic("to do")
}
