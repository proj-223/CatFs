package client

import (
	"errors"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"github.com/proj-223/CatFs/protocols/pool"
	"io"
	"os"
	"sync"
)

var (
	ErrFileHasOpened = errors.New("File has opened")
	ErrFileNotOpened = errors.New("File has not opened")
	ErrRead          = errors.New("Read Error")
	ErrWrite         = errors.New("Writer Error")
)

type CatFile struct {
	path         string
	filestatus   *proc.CatFileStatus
	lease        *proc.CatFileLease
	pool         *pool.ClientPool
	currentblock []byte
	blockOff     int64
	offset       int64
	lock         *sync.Mutex
	isEOF        bool
	conf         *config.MachineConfig
	opened       bool
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
	return self.ReadAt(b, self.offset)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off. It
// returns the number of bytes read and the error, if any. ReadAt always returns
// a non-nil error when n < len(b). At end of file, that error is io.EOF.
func (self *CatFile) ReadAt(b []byte, off int64) (n int, _ error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	n = 0
	blocksize := self.conf.BlockSize()
	blockStartOffset := self.blockOff * blocksize
	blockEndOffset := blockStartOffset + (int64)(len(self.currentblock))

	self.offset = off % blocksize
	self.blockOff = off / blocksize
	if len(self.currentblock) == 0 || (off < blockStartOffset) || (off >= blockEndOffset) {
		go self.GetNewBlock()
		return 0, ErrRead
	}

	for self.offset < (int64)(len(self.currentblock)) {
		if n >= len(b) {
			return n, nil
		}
		b[n] = self.currentblock[self.offset]
		n++
		self.offset++
	}

	if self.isEOF { // end of file
		self.offset = -1
		return n, io.EOF
	}

	self.offset = 0 // set offset to 0
	self.blockOff++ // set blockOffset to next block offset
	go self.GetNewBlock()
	return n, ErrRead
}

func (self *CatFile) GetNewBlock() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	master := self.pool.MasterServer()
	offset := self.conf.BlockSize() * self.blockOff
	blockquery := &proc.BlockQueryParam{
		Path:   self.path,
		Offset: offset,
		Length: self.conf.BlockSize(),
		Lease:  self.lease,
	}
	var resp proc.GetBlocksLocationResponse
	err := master.GetBlockLocation(blockquery, &resp)
	if err != nil && err != io.EOF {
		return err
	}

	self.isEOF = resp.EOF
	block := resp.Blocks[0]
	err = self.GetBlockData(block)
	if err != nil {
		return err
	}
	return nil
}

func (self *CatFile) GetBlockData(block *proc.CatBlock) error {
	location := block.Locations[0]
	dataServer := self.pool.DataServer(location) //*DataRPCClient
	var lease proc.CatLease
	param := &proc.GetBlockParam{
		Block: block,
	}
	err := dataServer.GetBlock(param, &lease)
	if err != nil {
		return err
	}

	blockClient := self.pool.NewBlockClient(location)
	ch := make(chan []byte)
	go blockClient.GetBlock(ch, lease.ID)
	for data := range ch {
		for _, value := range data {
			self.currentblock = append(self.currentblock, value)
		}
	}
	return nil
}

// Readdir reads the contents of the directory associated with file and returns
// a slice of up to n FileInfo values, as would be returned by Lstat, in
// directory order. Subsequent calls on the same file will yield further
// FileInfo.
//
// If n > 0, Readdir returns at most n FileInfo structures. In this case, if
// Readdir returns an empty slice, it will return a non-nil error explaining
// why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the FileInfo from the directory in a single
// slice. In this case, if Readdir succeeds (reads all the way to the end of the
// directory), it returns the slice and a nil error. If it encounters an error
// before the end of the directory, Readdir returns the FileInfo read until that
// point and a non-nil error.
func (self *CatFile) Readdir(n int) (fi []os.FileInfo, err error) {
	panic("to do")
}

// TODO
// func (self *CatFile) Readdirnames(n int) (name []string, err error) {
// }

// Seek sets the offset for the next Read or Write on file to offset,
// interpreted according to whence: 0 means relative to the origin of the file,
// 1 means relative to the current offset, and 2 means relative to the end. It
// returns the new offset and an error, if any.
func (self *CatFile) Seek(offset int64, whence int) (ret int64, err error) {
	panic("to do")
}

// Stat returns the FileInfo structure describing file. If there is an error, it
// will be of type *PathError.
func (self *CatFile) Stat(fi os.FileInfo, err error) {
	panic("to do")
}

// Sync commits the current contents of the file to stable storage. Typically, this
// means flushing the file system's in-memory copy of recently written data to
// disk.
func (self *CatFile) Sync() (err error) {
	panic("to do")
}

// Write writes len(b) bytes to the File. It returns the number of bytes written
// and an error, if any. Write returns a non-nil error when n != len(b).
func (self *CatFile) Write(b []byte) (n int, err error) {
	panic("to do")
}

// WriteAt writes len(b) bytes to the File starting at byte offset off. It
// returns the number of bytes written and an error, if any. WriteAt returns a
// non-nil error when n != len(b).
func (self *CatFile) WriteAt(b []byte, off int64) (n int, err error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	n = 0
	blocksize := self.conf.BlockServerConf.BlockSize
	blockStartOffset := self.blockOff * blocksize
	blockEndOffset := self.blockOff*blocksize + (int64)(len(self.currentblock))

	if len(self.currentblock) == 0 {
		self.offset = off
		self.blockOff = off / blocksize
		// go routine to send block
		return 0, ErrWrite
	}

	if (off < blockStartOffset) || (off >= blockEndOffset) {
		self.blockOff = off / blocksize
		self.offset = off
		//go routine here to send the required block
		return 0, ErrWrite
	}

	curoff := off - self.blockOff*blocksize
	for curoff < (int64)(len(self.currentblock)) {
		if n >= len(b) {
			self.offset = off + (int64)(n)
			return n, nil
		}

		self.currentblock[curoff] = b[n]
		n++
		curoff++
	}

	/*if self.isEOF == true { // don't have to care about the end of file ?
		self.offset = -1
		return n, io.EOF
	}*/

	self.offset = off + (int64)(n)
	self.blockOff += 1
	// go routine to send another block
	return n, ErrRead
}

func (self *CatFile) SendNewBlock() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	master := self.pool.MasterServer()
	param := &proc.AddBlockParam{
		Path:  self.path,
		Lease: self.lease,
	}
	var catblock proc.CatBlock
	err := master.AddBlock(param, &catblock)
	if err != nil {
		return err
	}

	err = self.WriteBlockData(&catblock)
	if err != nil {
		return err
	}

	return nil
}

func (self *CatFile) WriteBlockData(block *proc.CatBlock) error {
	location := block.Locations[0]
	dataServer := self.pool.DataServer(location) //*DataRPCClient
	var lease proc.CatLease
	param := &proc.GetBlockParam{
		Block: block,
	}
	err := dataServer.GetBlock(param, &lease)
	if err != nil {
		return err
	}

	blockClient := self.pool.NewBlockClient(location)
	transID := lease.ID
	ch := make(chan []byte)
	data := []byte{}
	self.currentblock = []byte{}
	go blockClient.GetBlock(ch, transID)
	for data = range ch {
		for _, value := range data {
			self.currentblock = append(self.currentblock, value)
		}
	}

	return nil
}
