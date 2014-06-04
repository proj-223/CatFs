package pool

import (
	"fmt"
	proc "github.com/proj-223/CatFs/protocols"
	"net/rpc"
	"sync"
)

type MasterRPCClient struct {
	conn *rpc.Client
	addr string
	lock *sync.Mutex
}

func (self *MasterRPCClient) Connect() error {
	var err error
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.conn == nil {
		self.conn, err = rpc.DialHTTP("tcp", self.addr)
	}
	return err
}

func (self *MasterRPCClient) CloseConn() error {
	return self.conn.Close()
}

func NewMasterClient(addr string) *MasterRPCClient {
	return &MasterRPCClient{addr: addr, lock: new(sync.Mutex)}
}

func (self *MasterRPCClient) Call(method string, args interface{}, reply interface{}) error {
	var err error
	method = fmt.Sprintf("MasterServer.%s", method)
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

// Get location of the block of the specified file within the specified range
func (self *MasterRPCClient) GetBlockLocation(query *proc.BlockQueryParam, blocks *proc.GetBlocksLocationResponse) error {
	return self.Call("GetBlockLocation", query, blocks)
}

// Create a file in a given path
func (self *MasterRPCClient) Create(param *proc.CreateFileParam, rep *proc.OpenFileResponse) error {
	return self.Call("Create", param, rep)
}

// Open a file to add block
func (self *MasterRPCClient) Open(param *proc.OpenFileParam, rep *proc.OpenFileResponse) error {
	return self.Call("Open", param, rep)
}

// Drop a block
func (self *MasterRPCClient) AbandonBlock(param *proc.AbandonBlockParam, succ *bool) error {
	return self.Call("AbandonBlock", param, succ)
}

// Add a block to a specific path (file)
func (self *MasterRPCClient) AddBlock(param *proc.AddBlockParam, block *proc.CatBlock) error {
	return self.Call("AddBlock", param, block)
}

// Complete an operation,
// delete the lease (lock)
func (self *MasterRPCClient) Close(param *proc.CloseParam, succ *bool) error {
	return self.Call("Close", param, succ)
}

// Rename
func (self *MasterRPCClient) Rename(param *proc.RenameParam, succ *bool) error {
	return self.Call("Rename", param, succ)
}

// Delete a file
func (self *MasterRPCClient) Delete(param *proc.DeleteParam, succ *bool) error {
	return self.Call("Delete", param, succ)
}

// Create a dir
func (self *MasterRPCClient) Mkdirs(param *proc.MkdirParam, succ *bool) error {
	return self.Call("Mkdirs", param, succ)
}

// List dir
func (self *MasterRPCClient) Listdir(param *proc.ListDirParam, files *proc.ListDirResponse) error {
	return self.Call("Listdir", param, files)
}

// Renew a lease
func (self *MasterRPCClient) RenewLease(oldLease *proc.CatFileLease, newLease *proc.CatFileLease) error {
	return self.Call("RenewLease", oldLease, newLease)
}

// File info
func (self *MasterRPCClient) GetFileInfo(path string, filestatus *proc.CatFileStatus) error {
	return self.Call("GetFileInfo", path, filestatus)
}

// Register a data server
func (self *MasterRPCClient) RegisterDataServer(param *proc.RegisterDataParam, succ *bool) error {
	return self.Call("RegisterDataServer", param, succ)
}

// Send heartbeat to master
func (self *MasterRPCClient) SendHeartbeat(param *proc.HeartbeatParam, rep *proc.HeartbeatResponse) error {
	return self.Call("SendHeartbeat", param, rep)
}

// Send blockreport to master
func (self *MasterRPCClient) BlockReport(param *proc.BlockReportParam, rep *proc.BlockReportResponse) error {
	return self.Call("BlockReport", param, rep)
}
