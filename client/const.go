package client

import (
	"errors"
	"github.com/proj-223/CatFs/config"
	"github.com/proj-223/CatFs/protocols/pool"
)

var (
	ErrInvalidPath  = errors.New("Invalid Path")
	ErrInvalidParam = errors.New("Invalid Param")
	ErrNoBlocks     = errors.New("No Blocks")
)

func NewCatClient(conf *config.MachineConfig) *CatClient {
	return &CatClient{
		pool:   pool.NewClientPool(conf),
		conf:   conf,
		curdir: "/",
	}
}
