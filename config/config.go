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
	HeartBeatInterval time.Duration
}

type MachineConfig struct {
	master      *MasterConfig
	dataServers []*DataServerConfig
	general     *GeneralConfig
}

func (self *MachineConfig) MasterPort() string {
	return self.master.Port
}

func (self *MachineConfig) DataServerPort(index int) string {
	return self.dataServers[index].Port
}

func (self *MachineConfig) BlockServerPort(index int) string {
	return self.dataServers[index].BlockServerPort
}

func (self *MachineConfig) MasterAddr() string {
	return fmt.Sprintf("%s:%s", self.master.Host, self.master.Port)
}

func (self *MachineConfig) DataServerAddr(index int) string {
	return fmt.Sprintf("%s:%s", self.dataServers[index].Host,
		self.dataServers[index].Port)
}

func (self *MachineConfig) DataServerHost(index int) string {
	return self.dataServers[index].Host
}

func (self *MachineConfig) DataServerAddrs() []string {
	var addrs []string
	for i := range self.dataServers {
		addrs = append(addrs, self.DataServerAddr(i))
	}
	return addrs
}

func (self *MachineConfig) BlockServerAddr(index int) string {
	return fmt.Sprintf("%s:%s", self.dataServers[index].Host, self.dataServers[index].BlockServerPort)
}

func (self *MachineConfig) BlockSize() int64 {
	return self.general.BlockSize
}

func (self *MachineConfig) BlockPath(index int) string {
	return self.dataServers[index].BlockPath
}

func (self *MachineConfig) ReplicaCount() int {
	return self.general.ReplicaCount
}

func (self *MachineConfig) HeartBeatInterval() time.Duration {
	return self.general.HeartBeatInterval
}

const (
	DefaultHost = "localhost"
)

var (
	DefaultMasterConfig = &MasterConfig{
		Host: DefaultHost,
		Port: "10000",
	}
	DefaultGeneralConfig = &GeneralConfig{
		BlockSize:         1 << 20,
		ReplicaCount:      3,
		HeartBeatInterval: 10 * time.Second,
	}
	DefaultMachineConfig = &MachineConfig{
		master:  DefaultMasterConfig,
		general: DefaultGeneralConfig,
		dataServers: []*DataServerConfig{
			DefaultDataServerConfig("10001"),
			DefaultDataServerConfig("10002"),
			DefaultDataServerConfig("10003"),
			DefaultDataServerConfig("10004"),
		},
	}
)

func DefaultDataServerConfig(port string) *DataServerConfig {
	return &DataServerConfig{
		Host:            DefaultHost,
		Port:            port,
		BlockServerPort: "2" + port[1:],
		BlockPath:       "/tmp/cat-fs-blocks/" + port,
	}
}
