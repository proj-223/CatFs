package master

import (
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type MasterRPC struct {
}

// Get location of the block of the specified file within the specified range
func (self *MasterRPC) GetBlockLocation(query *proc.BlockQueryParam, resp *proc.GetBlocksLocationResponse) error {
	blocks, eof, err := catFileSystem.QueryBlocks(query.Path, query.Offset, query.Length)
	if err != nil {
		return err
	}
	resp.EOF = eof
	resp.Blocks = blocks.ToCatBlock()
	return nil
}

// Create a file in a given path
func (self *MasterRPC) Create(param *proc.CreateFileParam, resp *proc.OpenFileResponse) error {
	fi, err := catFileSystem.CreateFile(param.Path, 0)
	if err != nil {
		return err
	}
	resp.Filestatus = fi.Status()
	// TODO open lock
	// TODO put the lease
	resp.Lease = proc.NewFileLease(proc.LEASE_WRITE)
	return nil
}

// Open a file to add block
func (self *MasterRPC) Open(param *proc.OpenFileParam, resp *proc.OpenFileResponse) error {
	fi, err := catFileSystem.GetFile(param.Path)
	if err != nil {
		return err
	}
	resp.Filestatus = fi.Status()
	// TODO open lock
	// TODO put the lease
	resp.Lease = proc.NewFileLease(proc.LEASE_WRITE)
	return nil
}

// Delete a block from a file
func (self *MasterRPC) AbandonBlock(param *proc.AbandonBlockParam, succ *bool) error {
	*succ = true
	panic("to do")
	return nil
}

// Add a block to a specific path (file)
func (self *MasterRPC) AddBlock(param *proc.AddBlockParam, block *proc.CatBlock) error {
	b, err := catFileSystem.AddBlock(param.Path)
	if err != nil {
		return err
	}
	block.ID = b.ID()
	block.Locations = b.Locations()
	return nil
}

// Complete an operation,
// delete the lease (lock)
func (self *MasterRPC) Close(param *proc.CloseParam, succ *bool) error {
	// TODO release the lease
	*succ = true
	return nil
}

// Rename
func (self *MasterRPC) Rename(param *proc.RenameParam, succ *bool) error {
	*succ = true
	return catFileSystem.Rename(param.Src, param.Des)
}

// Delete a file
func (self *MasterRPC) Delete(param *proc.DeleteParam, succ *bool) error {
	*succ = true
	return catFileSystem.DeleteFile(param.Path)
}

// Create a dir
func (self *MasterRPC) Mkdirs(param *proc.MkdirParam, succ *bool) error {
	*succ = true
	_, err := catFileSystem.Mkdirs(param.Path, 0)
	return err
}

// List dir, why the return value is not a list?
func (self *MasterRPC) Listdir(param *proc.ListDirParam, resp *proc.ListDirResponse) error {
	fis, err := catFileSystem.ListDir(param.Path)
	if err != nil {
		return err
	}
	resp.Files = fis.Status()
	return nil
}

// Renew a lease
func (self *MasterRPC) RenewLease(oldLease *proc.CatFileLease, newLease *proc.CatFileLease) error {
	// TODO lease manager
	// newLease.Renew(oldLease)
	return nil
}

// File info
func (self *MasterRPC) GetFileInfo(path string, filestatus *proc.CatFileStatus) error {
	fe, err := catFileSystem.GetFileEntry(path)
	if err != nil {
		return err
	}
	*filestatus = *(fe.Status())
	return nil
}

// Register a data server
func (self *MasterRPC) RegisterDataServer(param *proc.RegisterDataParam, succ *bool) error {
	*succ = true
	log.Printf("DataServer %d registered", param.Status.Location)
	slaveManager.RegisterSlave(param.Status)
	return nil
}

// Send heartbeat to master
func (self *MasterRPC) SendHeartbeat(param *proc.HeartbeatParam, rep *proc.HeartbeatResponse) error {
	slaveManager.UpdateSlave(param.Status)
	// TODO return commands
	return nil
}

// Send blockreport to master
func (self *MasterRPC) BlockReport(param *proc.BlockReportParam, rep *proc.BlockReportResponse) error {
	panic("to do")
}

func initMasterRPC(done chan error) {
	server := rpc.NewServer()
	server.Register(new(MasterRPC))
	l, err := net.Listen("tcp", ":"+config.MasterPort())
	if err != nil {
		done <- err
		return
	}
	log.Printf(START_MSG, config.MasterAddr())
	err = http.Serve(l, server)
	done <- err
}
