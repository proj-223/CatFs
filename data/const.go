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
	ErrInvalidLease    = errors.New("Invalid Lease")
	ErrInvalidPath     = errors.New("Invalid Path")
	ErrOperationFailed = errors.New("Operation Failed")
)

func Serve(index int) error {
	server := NewDataServer(config.DefaultMachineConfig, proc.ServerLocation(index))
	return server.Serve()
}

// Create a new Master Server
func NewDataServer(conf *config.MachineConfig, location proc.ServerLocation) *DataServer {
	leaseManager := NewLeaseManager()
	ds := &DataServer{
		pool:         pool.NewClientPool(conf),
		conf:         conf,
		location:     location,
		blockServer:  NewBlockServer(location, conf, leaseManager),
		pipelineMap:  make(map[string]*PipelineParam),
		leaseMap:     make(map[string]*proc.CatLease),
		leaseManager: leaseManager,
		commands:     make(chan *proc.MasterCommand, DEFAULT_CHAN_SIZE),
	}
	ds.registerLeaseListener()
	return ds
}
