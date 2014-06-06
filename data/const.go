package data

import (
	"errors"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"github.com/proj-223/CatFs/protocols/pool"
	"time"
)

const (
	RPC_START_MSG = "CatFS Data Server %d RPC are start: %s\n"
)

const (
	DEFAULT_CHAN_SIZE = 10
)

var (
	DEFAULT_TIMEOUT = time.Second * 30
)

var (
	ErrInvalidLease = errors.New("Invalid Lease")
	ErrInvalidPath  = errors.New("Invalid Path")
)

var (
	DefaultDataServers []*DataServer = []*DataServer{
		NewDataServer(config.DefaultMachineConfig, 0),
		NewDataServer(config.DefaultMachineConfig, 1),
		NewDataServer(config.DefaultMachineConfig, 2),
		NewDataServer(config.DefaultMachineConfig, 3),
	}
)

func Serve(index int) error {
	return DefaultDataServers[index].Serve()
}

// Create a new Master Server
func NewDataServer(conf *config.MachineConfig, index int) *DataServer {
	leaseManager := NewLeaseManager()
	ds := &DataServer{
		pool:         pool.NewClientPool(conf),
		conf:         conf,
		index:        index,
		blockServer:  NewBlockServer(conf.BlockServerConf, leaseManager),
		pipelineMap:  make(map[string]*PipelineParam),
		leaseMap:     make(map[string]*proc.CatLease),
		leaseManager: leaseManager,
		commands:     make(chan *proc.MasterCommand, DEFAULT_CHAN_SIZE),
	}
	ds.registerLeaseListener()
	return ds
}
