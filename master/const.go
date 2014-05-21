package master

import (
	"github.com/proj-223/CatFs/config"
)

var (
	DefaultMaster = NewMasterServer(config.DefaultMachineConfig)
)

func Init() error {
	return DefaultMaster.Init()
}

// Create a new Master Server
func NewMasterServer(conf *config.MachineConfig) *Master {
	panic("to do")
}
