package master

import (
	"github.com/proj-223/CatFs/config"
)

const (
	START_MSG = "CatFS Master RPC are start: %s"
)

var (
	DefaultMaster = NewMasterServer(config.DefaultMachineConfig)
)

func Serve() error {
	return ServeMaster(DefaultMaster)
}

// Init the Master Server
func ServeMaster(master *Master) error {
	done := make(chan error, 1)

	// init the rpc server
	go master.initRPCServer(done)

	err := <-done
	return err
}

// Create a new Master Server
func NewMasterServer(conf *config.MachineConfig) *Master {
	master := &Master{
		conf: conf,
	}
	return master
}
