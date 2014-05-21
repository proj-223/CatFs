package data

import (
	"github.com/proj-223/CatFs/config"
)

var (
	DefaultDataServer = NewDataServer(config.DefaultMachineConfig)
)

func Init() error {
	return DefaultDataServer.Init()
}

// Create a new Master Server
func NewDataServer(conf *config.MachineConfig) *DataServer {
	panic("to do")
}
