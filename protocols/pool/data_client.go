package pool

import (
	"fmt"
	proc "github.com/proj-223/CatFs/protocols"
	"net/rpc"
	"sync"
)

type DataRPCClient struct {
	conn *rpc.Client
	addr string
	lock *sync.Mutex
}

func (self *DataRPCClient) Connect() error {
	var err error
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.conn == nil {
		self.conn, err = rpc.DialHTTP("tcp", self.addr)
	}
	return err
}

func (self *DataRPCClient) CloseConn() error {
	return self.conn.Close()
}

func NewDataClient(addr string) *DataRPCClient {
	return &DataRPCClient{addr: addr, lock: new(sync.Mutex)}
}

func (self *DataRPCClient) Call(method string, args interface{}, reply interface{}) error {
	var err error
	method = fmt.Sprintf("DataServer.%s", method)
	for i := 0; i < MAX_RECONNECT; i++ {
		if self.conn != nil {
			err = self.conn.Call(method, args, reply)
			if err == nil {
				// call success
				return nil
			}
			if err != rpc.ErrShutdown {
				break
			}
		}
		err = self.Connect()
		if err != nil {
			break
		}
	}
	self.conn = nil
	if err != nil {
		return err
	}
	return rpc.ErrShutdown
}

// Prepare send a block to datanode
func (self *DataRPCClient) PrepareSendBlock(param *proc.PrepareBlockParam, lease *proc.CatLease) error {
	return self.Call("PrepareSendBlock", param, lease)
}

// Wait util blocks reach destination
// The block will be sent by a pipeline
func (self *DataRPCClient) SendingBlock(param *proc.SendingBlockParam, succ *bool) error {
	return self.Call("SendingBlock", param, succ)
}

// Get the block from data server
// Will start an tcp connect to request block
func (self *DataRPCClient) GetBlock(param *proc.GetBlockParam, lease *proc.CatLease) error {
	return self.Call("GetBlock", param, lease)
}
