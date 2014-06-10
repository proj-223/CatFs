package fs

import (
	"github.com/proj-223/CatFs/client"
	"github.com/proj-223/CatFs/config"
	"github.com/proj-223/CatFs/data"
	"github.com/proj-223/CatFs/master"
	proc "github.com/proj-223/CatFs/protocols"
)

func NewClient(conf *config.MachineConfig) *client.CatClient {
	return client.NewCatClient(conf)
}

func NewMasterServer(conf *config.MachineConfig) *master.Master {
	return master.NewMasterServer(conf)
}

func NewDataServer(conf *config.MachineConfig, location proc.ServerLocation) *data.DataServer {
	return data.NewDataServer(conf, location)
}
