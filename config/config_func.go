package config

var conf *MachineConfig

func LoadConfig(path string) {
	conf = DefaultMachineConfig
}

func ReplicaCount() int {
	return conf.ReplicaCount()
}
