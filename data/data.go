package data

import (
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"github.com/proj-223/CatFs/protocols/pool"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type PipelineParam struct {
	lease    *proc.CatLease
	location proc.ServerLocation
}

func (self *PipelineParam) HasInit() bool {
	return self.lease.HasInit()
}

func (self *PipelineParam) NextSendingParam() *proc.SendingBlockParam {
	return &proc.SendingBlockParam{
		Lease: self.lease,
	}
}

func NewPipelineParam(lease *proc.CatLease, param *proc.PrepareBlockParam) *PipelineParam {
	p := &PipelineParam{
		lease: lease,
	}
	if param != nil {
		p.location = param.ServerLocation()
	}
	return p
}

type DataServer struct {
	pool         *pool.ClientPool
	conf         *config.MachineConfig
	location     proc.ServerLocation
	blockServer  *BlockServer
	pipelineMap  map[string]*PipelineParam
	leaseMap     map[string]*proc.CatLease
	leaseManager *LeaseManager
	commands     chan *proc.MasterCommand
}

// Prepare send a block to datanode
func (self *DataServer) PrepareSendBlock(param *proc.PrepareBlockParam, lease *proc.CatLease) error {
	var nextLease proc.CatLease
	var deliverChan chan []byte
	nextParam := param.NextPipeParam()
	if nextParam != nil {
		// if there is another replica
		location := nextParam.ServerLocation()
		nextDataServer := self.pool.DataServer(location)
		// prepare next data server
		err := nextDataServer.PrepareSendBlock(nextParam, &nextLease)
		if err != nil {
			return err
		}
		// prepare deliverChan block to next data server
		nextBlockClient := self.pool.NewBlockClient(location)
		deliverChan := make(chan []byte)
		go nextBlockClient.SendBlock(deliverChan, nextLease.ID)
	}

	writeDiskChan := make(chan []byte, DEFAULT_CHAN_SIZE)
	done := make(chan bool, 1)

	// init the lease
	lease.New()
	self.leaseMap[lease.ID] = lease
	self.pipelineMap[lease.ID] = NewPipelineParam(&nextLease, nextParam)

	trans := NewReadTransaction(lease.ID, done, deliverChan, writeDiskChan)
	go self.writeBlockToDisk(writeDiskChan, param.Block)
	// init data receiver
	self.blockServer.StartTransaction(trans)
	return nil
}

// Wait util blocks reach destination
// The block will be sent by a pipeline
func (self *DataServer) SendingBlock(param *proc.SendingBlockParam, succ *bool) error {
	lease := param.Lease
	// anyway remove the lease
	defer self.leaseManager.RemoveLease(lease)

	next, ok := self.pipelineMap[lease.ID]
	if !ok {
		return ErrInvalidLease
	}
	if next.HasInit() {
		nextParam := next.NextSendingParam()
		nextDataServer := self.pool.DataServer(next.location)
		err := nextDataServer.SendingBlock(nextParam, succ)
		if err != nil || !*succ {
			return err
		}
	}
	trans := self.blockServer.Transaction(lease.ID)
	select {
	case <-trans.Done:
		*succ = true
	case <-time.After(DEFAULT_TIMEOUT):
		*succ = false
	}
	return nil
}

// Get the block from data server
// Will start an tcp connect to request block
func (self *DataServer) GetBlock(param *proc.GetBlockParam, lease *proc.CatLease) error {
	// init lease
	lease.New()
	self.leaseMap[lease.ID] = lease

	block := param.Block
	data := make(chan []byte)
	go self.readBlockFromDisk(data, block)
	trans := NewProviderTransaction(lease.ID, data)
	self.blockServer.StartTransaction(trans)
	return nil
}

func (self *DataServer) addr() string {
	return self.conf.DataServerAddr(int(self.location))
}

// go routine to init the data rpc server
func (self *DataServer) initRPCServer(done chan error) {
	server := rpc.NewServer()
	server.Register(proc.DataProtocol(self))
	l, err := net.Listen("tcp", self.addr())
	if err != nil {
		done <- err
		return
	}
	log.Printf(RPC_START_MSG, self.location, self.addr())
	err = http.Serve(l, server)
	done <- err
}

func (self *DataServer) initBlockServer(done chan error) {
	err := self.blockServer.Serve()
	done <- err
}

func (self *DataServer) Serve() error {
	done := make(chan error, 1)

	self.initBlockDir()
	// register, check and send heart beat
	go self.examServer(done)
	go self.initCommandHandler()
	// init the rpc server
	go self.initRPCServer(done)
	// init the block server
	go self.initBlockServer(done)
	// check leases
	go self.leaseManager.checkLease()

	err := <-done
	log.Printf("Log err %s\n", err.Error())
	return err
}
