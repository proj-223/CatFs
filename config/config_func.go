package config

import (
	"time"
)

var conf *MachineConfig = DefaultMachineConfig

func LoadConfig(path string) {
	conf = DefaultMachineConfig
}

func ReplicaCount() int {
	return conf.ReplicaCount()
}

func BlockSize() int64 {
	return conf.BlockSize()
}

func MasterPort() string {
	return conf.MasterPort()
}

func MasterAddr() string {
	return conf.MasterAddr()
}

func DataServerPort(index int) string {
	return conf.DataServerPort(index)
}

func BlockServerPort(index int) string {
	return conf.BlockServerPort(index)
}

func DataServerAddr(index int) string {
	return conf.DataServerAddr(index)
}

func DataServerHost(index int) string {
	return conf.DataServerHost(index)
}

func DataServerAddrs() []string {
	return conf.DataServerAddrs()
}

func BlockServerAddr(index int) string {
	return conf.BlockServerAddr(index)
}

func BlockPath(index int) string {
	return conf.BlockPath(index)
}

func HeartBeatInterval() time.Duration {
	return conf.HeartBeatInterval()
}
