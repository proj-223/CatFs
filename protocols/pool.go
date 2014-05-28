package protocols

import (
	"github.com/proj-223/CatFs/config"
)

var (
	DefaultClientPool = NewClientPool(config.DefaultMachineConfig)
)

func MasterServer() *MasterRPCClient {
	return DefaultClientPool.MasterServer()
}

func DataServer(index int) *DataRPCClient {
	return DefaultClientPool.DataServer(index)
}

func Close() {
	DefaultClientPool.Close()
}

type ClientPool struct {
	master      *MasterRPCClient
	dataServers []*DataRPCClient
}

// Get the Master Server Client
func (self *ClientPool) MasterServer() *MasterRPCClient {
	return self.master
}

// Get the Data Server Client
func (self *ClientPool) DataServer(index int) *DataRPCClient {
	if index >= len(self.dataServers) {
		return nil
	}
	return self.dataServers[index]
}

// Get the Data Server Client
func (self *ClientPool) Close() {
	self.master.CloseConn()
	for _, ds := range self.dataServers {
		ds.CloseConn()
	}
}

// init a new Client Pool
func NewClientPool(conf *config.MachineConfig) *ClientPool {
	cp := new(ClientPool)
	cp.master = NewMasterClient(conf.MasterAddr())
	addrs := conf.DataServerAddrs()
	for _, addr := range addrs {
		cp.dataServers = append(cp.dataServers, NewDataClient(addr))
	}
	return cp
}
