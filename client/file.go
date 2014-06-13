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
	ErrFileHasOpened = errors.New("File has opened")
	ErrFileNotOpened = errors.New("File has not opened")
	ErrWrite         = errors.New("Writer Error")
)

type CatFile struct {
	path            string
	filestatus      *proc.CatFileStatus
	lease           *proc.CatFileLease
	pool            *pool.ClientPool
	curBlockContent []byte
	curBlock        *proc.CatBlock
	curBlockOff     int64
	curChanged      bool
	fileOffset      int64
	lock            *sync.Mutex
	isEOF           bool
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
	self.fileOffset = 0
	self.curBlockOff = -1
	self.isEOF = false
	self.opened = true
	self.lock = new(sync.Mutex)
	return nil
}

func (self *CatFile) Create() error {
	master := self.pool.MasterServer()
	param := &proc.CreateFileParam{
		Path: self.path,
	}
	var resp proc.OpenFileResponse
	err := master.Create(param, &resp)
	if err != nil {
		return err
	}
	self.filestatus = resp.Filestatus
	self.lease = resp.Lease
	self.fileOffset = 0
	self.curBlockOff = -1
	self.isEOF = false
	self.opened = true
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
	self.Sync()
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
	return self.ReadAt(b, self.fileOffset)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off. It
// returns the number of bytes read and the error, if any. ReadAt always returns
// a non-nil error when n < len(b). At end of file, that error is io.EOF.
func (self *CatFile) ReadAt(b []byte, off int64) (int, error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	// blockOffset of off
	blockOff := off / config.BlockSize()
	err := self.getBlock(blockOff)
	if err != nil {
		return 0, err
	}

	// offset of off in a block
	offset := off % config.BlockSize()
	dataRead := 0
	for {
		n := copy(b[dataRead:], self.curBlockContent[self.offset():])
		dataRead += n
		// if read enough data
		if dataRead == len(b) {
			offset = int64(n)
			break
		}
		// if it is the end of file
		if self.isEOF {
			self.setFileOffset(blockOff, int64(len(self.curBlockContent)))
			return dataRead, io.EOF
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
	self.setFileOffset(blockOff, offset)
	return dataRead, nil
}

func (self *CatFile) getBlock(blockOff int64) error {
	// if current block offset is the one we want to get
	if self.curBlockOff == blockOff {
		return nil
	}
	err := self.Sync()
	if err != nil {
		return err
	}
	master := self.pool.MasterServer()
	blockquery := &proc.BlockQueryParam{
		Path:   self.path,
		Offset: config.BlockSize() * blockOff,
		Length: config.BlockSize(),
		Lease:  self.lease,
	}
	// get block meta data
	var resp proc.GetBlocksLocationResponse
	err = master.GetBlockLocation(blockquery, &resp)
	if err != nil {
		return err
	}
	if len(resp.Blocks) == 0 {
		self.curBlock = nil
		return ErrNoBlocks
	}

	// contact data server
	location := resp.Blocks[0].Locations[0]
	dataServer := self.pool.DataServer(location)
	var lease proc.CatLease
	param := &proc.GetBlockParam{
		Block: resp.Blocks[0],
	}
	err = dataServer.GetBlock(param, &lease)
	if err != nil {
		return err
	}
	// get data
	blockClient := self.pool.NewBlockClient(location)
	ch := make(chan []byte)
	go blockClient.GetBlock(ch, lease.ID)
	var blockContent []byte
	for data := range ch {
		blockContent = append(blockContent, data...)
	}
	// set EOF and curBlock info
	self.isEOF = resp.EOF
	self.curBlock = resp.Blocks[0]
	self.curBlockContent = blockContent
	self.curBlockOff = blockOff
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
	if !self.curChanged {
		return nil
	}
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
	self.curChanged = false
	return nil
}

// Write writes len(b) bytes to the File. It returns the number of bytes written
// and an error, if any. Write returns a non-nil error when n != len(b).
func (self *CatFile) Write(b []byte) (int, error) {
	return self.WriteAt(b, self.fileOffset)
}

// WriteAt writes len(b) bytes to the File starting at byte offset off. It
// returns the number of bytes written and an error, if any. WriteAt returns a
// non-nil error when n != len(b).
func (self *CatFile) WriteAt(b []byte, off int64) (int, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	dataWrite := 0
	// blockOffset of off
	blockOff := off / config.BlockSize()
	// ceiling of length / blocksize
	fileBlockNumber := self.blockNumber()
	offset := off % config.BlockSize()

	for {
		// if it is the last block or more
		if blockOff >= fileBlockNumber-1 {
			offset += (blockOff - fileBlockNumber + 1) * config.BlockSize()
			n, err := self.appendToLastBlock(b, offset)
			dataWrite += n
			// read enough or there is an err
			if err != nil || dataWrite == len(b) {
				return dataWrite, err
			}
			n, err = self.appendBlock(b[dataWrite:])
			return n + dataWrite, err
		}
		// get the block of blockOff
		err := self.getBlock(blockOff)
		if err != nil {
			return dataWrite, err
		}
		// this should work, because the size of curBlockContent should be the block size
		n := copy(self.curBlockContent[offset:], b[dataWrite:])
		dataWrite += n
		// set current has changed
		self.curChanged = true
		if dataWrite == len(b) {
			offset = int64(n)
			break
		}
		offset = 0
		blockOff++
		self.Sync()
		self.setFileOffset(blockOff, offset)
	}
	self.setFileOffset(blockOff, offset)
	return dataWrite, nil
}

func (self *CatFile) appendToLastBlock(b []byte, offset int64) (int, error) {
	blockOff := self.blockNumber() - 1 // index of the last block
	err := self.getBlock(blockOff)
	if err == ErrNoBlocks {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(self.curBlockContent) == int(config.BlockSize()) {
		// if the last block is full
		// nothing to write
		return 0, nil
	}
	if offset > int64(len(self.curBlockContent)) {
		offset = int64(len(self.curBlockContent))
	}
	blockRemain := int(config.BlockSize() - offset)
	self.curChanged = true
	if blockRemain >= len(b) {
		self.curBlockContent = append(self.curBlockContent[:offset], b...)
		return len(b), nil
	}
	self.curBlockContent = append(self.curBlockContent[:offset], b[:blockRemain]...)
	return blockRemain, nil
}

func (self *CatFile) appendBlock(b []byte) (int, error) {
	dataWrite := 0
	n := 0
	blockOff := self.blockNumber() - 1 // index of the last block
	master := self.pool.MasterServer()
	for dataWrite < len(b) {
		err := self.Sync()
		if err != nil && dataWrite == 0 {
			return dataWrite, err
		}
		if err != nil {
			// && dataWrite != 0
			// write new block failed
			// TODO abandom block ?
			return dataWrite, err
		}
		blockContent := make([]byte, config.BlockSize())
		n = copy(blockContent, b[dataWrite:])
		dataWrite += n
		blockContent = blockContent[:n]
		blockOff++ // add block offset by 1

		param := &proc.AddBlockParam{
			Path:  self.path,
			Lease: self.lease,
		}
		var block proc.CatBlock
		err = master.AddBlock(param, &block)
		if err != nil {
			return dataWrite, err
		}
		self.curBlock = &block
		self.curBlockContent = blockContent
		self.curChanged = true
	}
	self.setFileOffset(blockOff, int64(len(self.curBlockContent)))
	return n, nil
}

func (self *CatFile) offset() int64 {
	return self.fileOffset % config.BlockSize()
}

func (self *CatFile) blockOffset() int64 {
	return self.fileOffset / config.BlockSize()
}

func (self *CatFile) setFileOffset(blockOff, offset int64) {
	self.fileOffset = blockOff*config.BlockSize() + offset
}

func (self *CatFile) blockNumber() int64 {
	return (self.filestatus.Length-1)/config.BlockSize() + 1
}
