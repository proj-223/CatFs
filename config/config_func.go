package config

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
