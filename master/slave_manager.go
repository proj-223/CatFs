package master

import (
	proc "github.com/proj-223/CatFs/protocols"
)

var (
	slaveManager = &SlaveManager{
		slaves: make(map[proc.ServerLocation]*Slave),
	}
)

type Slave struct {
	status *proc.DataServerStatus
	// the id of blocks
	blocks map[string]bool
}

type SlaveManager struct {
	slaves map[proc.ServerLocation]*Slave
}

func (self *SlaveManager) NewBlockReplica() []proc.ServerLocation {
	panic("to do")
}

func (self *SlaveManager) RegisterBlockReplica(block *Block) {
	panic("to do")
}

func (self *SlaveManager) RegisterSlave(status *proc.DataServerStatus) {
	panic("to do")
}
