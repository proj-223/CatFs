package config

import (
	"fmt"
)

type BlockServerConfig struct {
	Port      string
	BlockSize int64
}

type MasterConfig struct {
	Host string
	Port string
}

type DataServerConfig struct {
	Host string
	Port string
}

type MachineConfig struct {
	Master      *MasterConfig
	DataServers []*DataServerConfig
}

func (self *MachineConfig) MaterAddr() string {
	return fmt.Sprintf("%s:%s", self.Master.Host, self.Master.Port)
}

func (self *MachineConfig) DataServerAddr(index int) string {
	return fmt.Sprintf("%s:%s", self.DataServers[index].Host,
		self.DataServers[index].Port)
}

func (self *MachineConfig) DataServerAddrs() []string {
	var addrs []string
	for i := range self.DataServers {
		addrs = append(addrs, self.DataServerAddr(i))
	}
	return addrs
}

const (
	DefaultHost = "localhost"
)

var (
	DefaultMasterConfig = &MasterConfig{
		Host: DefaultHost,
		Port: "10000",
	}
	DefaultMachineConfig = &MachineConfig{
		Master: DefaultMasterConfig,
		DataServers: []*DataServerConfig{
			DefaultDataServerConfig("10001"),
			DefaultDataServerConfig("10002"),
			DefaultDataServerConfig("10003"),
			DefaultDataServerConfig("10004"),
		},
	}
)

func DefaultDataServerConfig(port string) *DataServerConfig {
	return &DataServerConfig{
		Host: DefaultHost,
		Port: port,
	}
}
