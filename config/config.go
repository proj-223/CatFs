package config

import (
	"fmt"
	"time"
)

type MasterConfig struct {
	Host string
	Port string
}

type DataServerConfig struct {
	Host            string
	Port            string
	BlockServerPort string
	BlockPath       string
}

type GeneralConfig struct {
	ReplicaCount      int
	BlockSize         int64
	HeartBeatInterval int
}

type MachineConfig struct {
	Master      *MasterConfig
	DataServers []*DataServerConfig
	General     *GeneralConfig
}

func (self *MachineConfig) MasterPort() string {
	return self.Master.Port
}

func (self *MachineConfig) DataServerPort(index int) string {
	return self.DataServers[index].Port
}

func (self *MachineConfig) BlockServerPort(index int) string {
	return self.DataServers[index].BlockServerPort
}

func (self *MachineConfig) MasterAddr() string {
	return fmt.Sprintf("%s:%s", self.Master.Host, self.Master.Port)
}

func (self *MachineConfig) DataServerAddr(index int) string {
	return fmt.Sprintf("%s:%s", self.DataServers[index].Host,
		self.DataServers[index].Port)
}

func (self *MachineConfig) DataServerHost(index int) string {
	return self.DataServers[index].Host
}

func (self *MachineConfig) DataServerAddrs() []string {
	var addrs []string
	for i := range self.DataServers {
		addrs = append(addrs, self.DataServerAddr(i))
	}
	return addrs
}

func (self *MachineConfig) BlockServerAddr(index int) string {
	return fmt.Sprintf("%s:%s", self.DataServers[index].Host, self.DataServers[index].BlockServerPort)
}

func (self *MachineConfig) BlockSize() int64 {
	return self.General.BlockSize
}

func (self *MachineConfig) BlockPath(index int) string {
	return self.DataServers[index].BlockPath
}

func (self *MachineConfig) ReplicaCount() int {
	return self.General.ReplicaCount
}

func (self *MachineConfig) HeartBeatInterval() time.Duration {
	return time.Duration(self.General.HeartBeatInterval) * time.Second
}

const (
	DefaultHost = "localhost"
)

var (
	DefaultMasterConfig = &MasterConfig{
		Host: "master.cse223.sheimi.me",
		Port: "10000",
	}
	DefaultGeneralConfig = &GeneralConfig{
		BlockSize:         1 << 20,
		ReplicaCount:      3,
		HeartBeatInterval: 5,
	}
	conf = &MachineConfig{
		Master:  DefaultMasterConfig,
		General: DefaultGeneralConfig,
		DataServers: []*DataServerConfig{
			DefaultDataServerConfig("data0.cse223.sheimi.me"),
			DefaultDataServerConfig("data1.cse223.sheimi.me"),
			DefaultDataServerConfig("data2.cse223.sheimi.me"),
		},
	}
)

func DefaultDataServerConfig(host string) *DataServerConfig {
	return &DataServerConfig{
		Host:            host,
		Port:            "10001",
		BlockServerPort: "20000",
		BlockPath:       "/tmp/cat-fs-blocks",
	}
}
