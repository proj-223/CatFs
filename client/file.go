package client

import (
	"errors"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"github.com/proj-223/CatFs/protocols/pool"
	"io"
	"log"
	"sync"
)

var (
	ErrFileHasOpened    = errors.New("File has opened")
	ErrFileNotOpened    = errors.New("File has not opened")
	ErrReadSmallerSize  = errors.New("Read Error")
	ErrWriteSmallerSize = errors.New("Read Error")
	ErrWrite            = errors.New("Writer Error")
)

type CatFile struct {
	path            string
	filestatus      *proc.CatFileStatus
	lease           *proc.CatFileLease
	pool            *pool.ClientPool
	curBlockContent []byte
	curBlock        *proc.CatBlock
	blockOff        int64
	offset          int64
	lock            *sync.Mutex
	isEOF           bool
	conf            *config.MachineConfig
	opened          bool
}

// open this file, if it is not opened
func (self *CatFile) Open(mode int) error {
	if self.opened {
		return ErrFileHasOpened
	}
	master := self.pool.MasterServer()
	opfileparam := &proc.OpenFileParam{
		Path: self.path,
		Mode: mode,
	}
	var resp proc.OpenFileResponse
	err := master.Open(opfileparam, &resp)
	if err != nil {
		return err
	}
	self.filestatus = resp.Filestatus
	self.lease = resp.Lease
	self.offset = 0
	self.blockOff = 0
	self.isEOF = false
	self.opened = false
	self.lock = new(sync.Mutex)
	return nil
}

// type io.Closer
// Close closes the File, rendering it unusable for I/O. It returns an error, if
// any.
func (self *CatFile) Close() error {
	if !self.opened {
		return ErrFileNotOpened
	}
	master := self.pool.MasterServer()
	param := &proc.CloseParam{
		Path:  self.path,
		Lease: self.lease,
	}
	var succ bool
	err := master.Close(param, &succ)
	if err != nil {
		return err
	}
	self.opened = true
	return nil
}

// IsExist returns a boolean indicating whether a file
// or directory already exists.
func (self *CatFile) IsExist() bool {
	return true
}

// IsDir returns a boolean indicating whether a file
// is a directory
func (self *CatFile) IsDir() bool {
	return false
}

// Read reads up to len(b) bytes from the File. It returns the number of bytes read
// and an error, if any. EOF is signaled by a zero count with err set to io.EOF.
func (self *CatFile) Read(b []byte) (int, error) {
	return self.ReadAt(b, self.fileOffset())
}

// ReadAt reads len(b) bytes from the File starting at byte offset off. It
// returns the number of bytes read and the error, if any. ReadAt always returns
// a non-nil error when n < len(b). At end of file, that error is io.EOF.
func (self *CatFile) ReadAt(b []byte, off int64) (n int, _ error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	n = 0
	// blockOffset of off
	blockOff := off / self.conf.BlockSize()
	if len(self.curBlockContent) == 0 || (self.blockOff != blockOff) {
		err := self.getBlock(blockOff)
		if err != nil {
			return 0, err
		}
	}

	// offset of off in a block
	offset := off % self.conf.BlockSize()
	for {
		// if read enough data
		if n >= len(b) {
			break
		}
		// copy data
		b[n] = self.curBlockContent[self.offset]
		n++
		offset++
		if offset < (int64)(len(self.curBlockContent)) {
			continue
		}
		// if it is the end of file
		if self.isEOF {
			self.offset = -1
			return n, io.EOF
		}
		// rest offset and blockOff
		offset = 0
		blockOff++
		// get next block
		err := self.getBlock(blockOff)
		if err != nil {
			return 0, err
		}
	}
	// set offset of the file
	self.offset = offset
	self.blockOff = blockOff
	return n, nil
}

func (self *CatFile) getBlock(blockOff int64) error {
	master := self.pool.MasterServer()
	blockquery := &proc.BlockQueryParam{
		Path:   self.path,
		Offset: self.conf.BlockSize() * blockOff,
		Length: self.conf.BlockSize(),
		Lease:  self.lease,
	}
	// get block meta data
	var resp proc.GetBlocksLocationResponse
	err := master.GetBlockLocation(blockquery, &resp)
	if err != nil {
		return err
	}
	// set EOF and curBlock info
	self.isEOF = resp.EOF
	self.curBlock = resp.Blocks[0]
	self.curBlockContent = nil

	// contact data server
	location := self.curBlock.Locations[0]
	dataServer := self.pool.DataServer(location)
	var lease proc.CatLease
	param := &proc.GetBlockParam{
		Block: self.curBlock,
	}
	err = dataServer.GetBlock(param, &lease)
	if err != nil {
		return err
	}
	// get data
	blockClient := self.pool.NewBlockClient(location)
	ch := make(chan []byte)
	go blockClient.GetBlock(ch, lease.ID)
	for data := range ch {
		for _, value := range data {
			self.curBlockContent = append(self.curBlockContent, value)
		}
	}
	return nil
}

// Seek sets the offset for the next Read or Write on file to offset,
// interpreted according to whence: 0 means relative to the origin of the file,
// 1 means relative to the current offset, and 2 means relative to the end. It
// returns the new offset and an error, if any.
func (self *CatFile) Seek(offset int64, whence int) (ret int64, err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	panic("to do")
}

// Sync commits the current contents of the file to stable storage. Typically, this
// means flushing the file system's in-memory copy of recently written data to
// disk.
func (self *CatFile) Sync() error {
	return self.writeData(self.curBlock, self.curBlockContent)
}

func (self *CatFile) writeData(block *proc.CatBlock, data []byte) error {
	location := block.Locations[0]
	dataServer := self.pool.DataServer(location)
	var lease proc.CatLease
	param := &proc.PrepareBlockParam{
		Block: block,
	}
	err := dataServer.PrepareSendBlock(param, &lease)
	if err != nil {
		log.Printf("Err sending block %s: ", err.Error())
		return err
	}
	blockClient := self.pool.NewBlockClient(location)
	go blockClient.SendBlockAll(data, lease.ID)
	sendingParam := &proc.SendingBlockParam{
		Lease: &lease,
	}
	var succ bool
	err = dataServer.SendingBlock(sendingParam, &succ)
	if err != nil {
		log.Printf("Err sending block %s: ", err.Error())
		return err
	}
	return nil
}

// Write writes len(b) bytes to the File. It returns the number of bytes written
// and an error, if any. Write returns a non-nil error when n != len(b).
func (self *CatFile) Write(b []byte) (n int, err error) {
	return self.WriteAt(b, self.fileOffset())
}

// WriteAt writes len(b) bytes to the File starting at byte offset off. It
// returns the number of bytes written and an error, if any. WriteAt returns a
// non-nil error when n != len(b).
func (self *CatFile) WriteAt(b []byte, off int64) (n int, err error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	return 0, nil
}

func (self *CatFile) fileOffset() int64 {
	return self.offset + self.conf.BlockSize()*self.blockOff
}
