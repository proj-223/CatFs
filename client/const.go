package client

import (
	"errors"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
)

var (
	ErrInvalidPath  = errors.New("Invalid Path")
	ErrInvalidParam = errors.New("Invalid Param")
)

func NewCatClient(conf *config.MachineConfig) *CatClient {
	return &CatClient{
		pool: proc.NewClientPool(conf),
		conf: conf,
    curdir: "/",
	}
}
