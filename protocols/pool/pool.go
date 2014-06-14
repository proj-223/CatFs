package pool

import (
	proc "github.com/proj-223/CatFs/protocols"
)

var (
	DefaultClientPool = NewClientPool()
)

func MasterServer() *MasterRPCClient {
	return DefaultClientPool.MasterServer()
}

func DataServer(index proc.ServerLocation) *DataRPCClient {
	return DefaultClientPool.DataServer(index)
}

func Close() {
	DefaultClientPool.Close()
}

type ClientPool struct {
	master      *MasterRPCClient
	dataServers map[proc.ServerLocation]*DataRPCClient
}

// Get the Master Server Client
func (self *ClientPool) MasterServer() *MasterRPCClient {
	// TODO add lock here
	if self.master == nil {
		self.master = NewMasterClient()
	}
	return self.master
}

// Get the Data Server Client
func (self *ClientPool) DataServer(index proc.ServerLocation) *DataRPCClient {
	// TODO add lock here
	if client, ok := self.dataServers[index]; ok {
		return client
	}
	client := NewDataClient(int(index))
	self.dataServers[index] = client
	return client
}

// Get new Block Client
func (self *ClientPool) NewBlockClient(index proc.ServerLocation) *BlockClient {
	client := NewBlockClient(int(index))
	return client
}

// Get the Data Server Client
func (self *ClientPool) Close() {
	self.master.CloseConn()
	for _, ds := range self.dataServers {
		ds.CloseConn()
	}
}

// init a new Client Pool
func NewClientPool() *ClientPool {
	cp := &ClientPool{
		dataServers: make(map[proc.ServerLocation]*DataRPCClient),
	}
	return cp
}
