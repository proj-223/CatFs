package protocols

import (
	"fmt"
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
func (self *MasterRPCClient) GetBlockLocation(query *BlockQueryParam, blocks *GetBlocksLocationResponse) error {
	return self.Call("GetBlockLocation", query, blocks)
}

// Create a file in a given path
func (self *MasterRPCClient) Create(param *CreateFileParam, response *OpenFileResponse) error {
	return self.Call("Create", param, response)
}

// Open a file to add block
func (self *MasterRPCClient) Open(param *OpenFileParam, response *OpenFileResponse) error {
	return self.Call("Open", param, response)
}

// Drop a block
func (self *MasterRPCClient) AbandonBlock(param *AbandonBlockParam, succ *bool) error {
	return self.Call("AbandonBlock", param, succ)
}

// Add a block to a specific path (file)
func (self *MasterRPCClient) AddBlock(param *AddBlockParam, block *CatBlock) error {
	return self.Call("AddBlock", param, block)
}

// Complete an operation,
// delete the lease (lock)
func (self *MasterRPCClient) Close(param *CloseParam, succ *bool) error {
	return self.Call("Close", param, succ)
}

// Rename
func (self *MasterRPCClient) Rename(param *RenameParam, succ *bool) error {
	return self.Call("Rename", param, succ)
}

// Delete a file
func (self *MasterRPCClient) Delete(param *DeleteParam, succ *bool) error {
	return self.Call("Delete", param, succ)
}

// Create a dir
func (self *MasterRPCClient) Mkdirs(param *MkdirParam, succ *bool) error {
	return self.Call("Mkdirs", param, succ)
}

// List dir
func (self *MasterRPCClient) Listdir(param *ListDirParam, files *ListdirResponse) error {
	return self.Call("Listdir", param, files)
}

// Renew a lease
func (self *MasterRPCClient) RenewLease(oldLease *CatFileLease, newLease *CatFileLease) error {
	return self.Call("RenewLease", oldLease, newLease)
}

// File info
func (self *MasterRPCClient) GetFileInfo(path string, filestatus *CatFileStatus) error {
	return self.Call("GetFileInfo", path, filestatus)
}

// Register a data server
func (self *MasterRPCClient) RegisterDataServer(param *RegisterDataParam, succ *bool) error {
	return self.Call("RegisterDataServer", param, succ)
}

// Send heartbeat to master
func (self *MasterRPCClient) SendHeartbeat(param *HeartbeatParam, rep *HeartbeatResponse) error {
	return self.Call("SendHeartbeat", param, rep)
}

// Send blockreport to master
func (self *MasterRPCClient) BlockReport(param *BlockReportParam, rep *BlockReportResponse) error {
	return self.Call("BlockReport", param, rep)
}
