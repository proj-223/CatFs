package data

import (
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"github.com/proj-223/CatFs/utils"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

const (
	DEFAULT_CHAN_SIZE = 10
	DEFAULT_TIMEOUT   = 30
)

type PipelineParam struct {
	lease    *proc.CatLease
	location proc.BlockLocation
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
		p.location = param.BlockLocation()
	}
	return p
}

type DataServer struct {
	pool        *proc.ClientPool
	conf        *config.MachineConfig
	index       int // index of this data server
	blockServer *proc.BlockServer
	pipelineMap map[string]*PipelineParam
}

// Prepare send a block to datanode
func (self *DataServer) PrepareSendBlock(param *proc.PrepareBlockParam, lease *proc.CatLease) error {
	var nextLease proc.CatLease
	var deliverChan chan []byte
	nextParam := param.NextPipeParam()
	if nextParam != nil {
		// if there is another replica
		nextDataServer := nextParam.BlockLocation().DataServer(self.pool)
		// prepare next data server
		err := nextDataServer.PrepareSendBlock(nextParam, &nextLease)
		if err != nil {
			return err
		}
		// prepare deliverChan block to next data server
		nextBlockClient := nextParam.BlockLocation().BlockClient(self.pool)
		deliverChan := make(chan []byte)
		go nextBlockClient.SendBlock(deliverChan, nextLease.ID)
	}

	writeDiskChan := make(chan []byte, DEFAULT_CHAN_SIZE)
	done := make(chan bool, 1)
	lease.New()
	self.pipelineMap[lease.ID] = NewPipelineParam(&nextLease, nextParam)
	trans := proc.NewReadTransaction(lease.ID, done, deliverChan, writeDiskChan)
	// init data receiver
	self.blockServer.StartTransaction(trans)
	return nil
}

// Wait util blocks reach destination
// The block will be sent by a pipeline
func (self *DataServer) SendingBlock(param *proc.SendingBlockParam, succ *bool) error {
	lease := param.Lease
	// anyway clean the lease
	defer self.cleanLease(lease)

	next, ok := self.pipelineMap[lease.ID]
	if !ok {
		return ErrInvalidLease
	}
	if next.HasInit() {
		nextParam := next.NextSendingParam()
		nextDataServer := next.location.DataServer(self.pool)
		err := nextDataServer.SendingBlock(nextParam, succ)
		if err != nil || !*succ {
			return err
		}
	}
	trans := self.blockServer.Transaction(lease.ID)
	timeout := utils.NewTimeout(DEFAULT_TIMEOUT)
	select {
	case <-trans.Done:
		*succ = true
	case <-timeout:
		*succ = false
	}
	return nil
}

// Get the block from data server
// Will start an tcp connect to request block
func (self *DataServer) GetBlock(param *proc.GetBlockParam, lease *proc.CatLease) error {
	panic("to do")
}

func (self *DataServer) addr() string {
	return self.conf.DataServerAddr(self.index)
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
	log.Printf(RPC_START_MSG, self.index, self.addr())
	err = http.Serve(l, server)
	done <- err
}

func (self *DataServer) initBlockServer(done chan error) {
	err := self.blockServer.Serve()
	done <- err
}
