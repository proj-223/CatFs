package client

import (
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
)

func NewCatClient(conf *config.MachineConfig) *CatClient {
	return &CatClient{
		pool: proc.NewClientPool(conf),
		conf: conf,
	}
}
