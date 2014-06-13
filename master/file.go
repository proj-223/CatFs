package master

import (
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"path"
	"sync"
)

const (
	DFS_FILETYPE_FILE DFSFileType = iota
	DFS_FILETYPE_DIR
)

var (
	rootDir = &CatDFSDir{
		filename: "/",
		children: make(map[string]DFSEntry),
		abspath:  "/",
		locker:   new(sync.RWMutex),
	}
)

type DFSFileType int

type DFSEntry interface {
	Delete() error

	Parent() DFSDir

	IsDir() bool

	RenameTo(dir DFSDir, filename string) error

	Filename() string

	Abs() string

	Status() *proc.CatFileStatus
}

type DFSFile interface {
	DFSEntry

	// return (blocklist, eof)
	QueryBlocks(offset int64, length int64) (BlockList, bool)

	AddBlock() (*Block, error)
}

type DFSDir interface {
	DFSEntry

	New(filename string, filetype DFSFileType, mode int) (DFSEntry, error)

	UnMountChild(child DFSEntry)

	MountChild(child DFSEntry) error

	// get file in the dir, return error when it is not dir or
	// there is no such file
	GetFile(filename string) (DFSEntry, error)

	List() DFSEntryList
}

type CatDFSFile struct {
	filename string
	abspath  string
	parent   DFSDir
	blocks   []*Block
	locker   *sync.RWMutex
}

func (self *CatDFSFile) QueryBlocks(offset int64, length int64) (BlockList, bool) {
	blockSize := config.BlockSize()
	startBlockOff := offset / blockSize
	endBlockOff := (offset+length-1)/blockSize + 1 // not include
	eof := false
	if endBlockOff >= int64(len(self.blocks)) {
		endBlockOff = int64(len(self.blocks))
		eof = true
	}
	return self.blocks[startBlockOff:endBlockOff], eof
}

func (self *CatDFSFile) Status() *proc.CatFileStatus {
	status := &proc.CatFileStatus{
		Filename: self.filename,
		Length:   int64(len(self.blocks)) * config.BlockSize(),
		IsDir:    false,
	}
	return status
}

func (self *CatDFSFile) AddBlock() (*Block, error) {
	block := blockManager.New(self)
	// TODO add a rpc call and call after write
	blockManager.Register(block)
	self.blocks = append(self.blocks, block)
	return block, nil
}

func (self *CatDFSFile) GetFile(filename string) (DFSEntry, error) {
	return nil, ErrNotDir
}

func (self *CatDFSFile) Delete() error {
	self.locker.Lock()
	defer self.locker.Unlock()
	for _, block := range self.blocks {
		blockManager.Remove(block.ID())
	}
	self.parent.UnMountChild(self)
	return nil
}

func (self *CatDFSFile) Parent() DFSDir {
	return self.parent
}

func (self *CatDFSFile) IsDir() bool {
	return false
}

func (self *CatDFSFile) RenameTo(dir DFSDir, filename string) error {
	self.parent.UnMountChild(self)

	self.locker.Lock()
	self.parent = dir
	self.filename = filename
	self.locker.Unlock()

	return dir.MountChild(self)
}

func (self *CatDFSFile) Filename() string {
	return self.filename
}

func (self *CatDFSFile) UnMountChild(child DFSEntry) error {
	return ErrNotDir
}

func (self *CatDFSFile) Abs() string {
	return self.abspath
}

type CatDFSDir struct {
	filename string
	abspath  string
	parent   DFSDir
	children map[string]DFSEntry
	locker   *sync.RWMutex
}

func (self *CatDFSDir) GetFile(filename string) (DFSEntry, error) {
	if fi, ok := self.children[filename]; ok {
		return fi, nil
	}
	return nil, ErrNoSuchFile
}

func (self *CatDFSDir) Delete() error {
	for _, fi := range self.children {
		fi.Delete()
	}
	if self.parent != nil {
		self.parent.UnMountChild(self)
	}
	return nil
}

func (self *CatDFSDir) Parent() DFSDir {
	return self.parent
}

func (self *CatDFSDir) New(filename string, filetype DFSFileType, mode int) (DFSEntry, error) {
	if _, ok := self.children[filename]; ok {
		return nil, ErrFileAlreadyExist
	}

	switch filetype {
	case DFS_FILETYPE_FILE:
		return self.newFile(filename, mode), nil
	case DFS_FILETYPE_DIR:
		return self.newDir(filename, mode), nil
	}
	return nil, ErrUnKnownFileType
}

func (self *CatDFSDir) IsDir() bool {
	return true
}

func (self *CatDFSDir) RenameTo(dir DFSDir, filename string) error {
	if self.parent == nil {
		return ErrIsRoot
	}

	self.parent.UnMountChild(self)

	self.locker.Lock()
	self.parent = dir
	self.filename = filename
	self.locker.Unlock()

	return dir.MountChild(self)
}

func (self *CatDFSDir) Filename() string {
	return self.filename
}

func (self *CatDFSDir) UnMountChild(child DFSEntry) {
	self.locker.Lock()
	defer self.locker.Unlock()
	if _, ok := self.children[child.Filename()]; ok {
		delete(self.children, child.Filename())
	}
}

func (self *CatDFSDir) MountChild(child DFSEntry) error {
	if _, ok := self.children[child.Filename()]; ok {
		return ErrFileAlreadyExist
	}
	self.locker.Lock()
	defer self.locker.Unlock()
	self.children[child.Filename()] = child
	return nil
}

func (self *CatDFSDir) newFile(filename string, mode int) DFSEntry {
	fi := &CatDFSFile{
		filename: filename,
		parent:   self,
		abspath:  path.Join(self.abspath, filename),
		locker:   new(sync.RWMutex),
	}
	self.locker.Lock()
	defer self.locker.Unlock()
	self.children[filename] = fi
	return fi
}

func (self *CatDFSDir) Abs() string {
	return self.abspath
}

func (self *CatDFSDir) newDir(filename string, mode int) DFSEntry {
	fd := &CatDFSDir{
		filename: filename,
		parent:   self,
		children: make(map[string]DFSEntry),
		abspath:  path.Join(self.abspath, filename),
		locker:   new(sync.RWMutex),
	}
	self.locker.Lock()
	defer self.locker.Unlock()
	self.children[filename] = fd
	return fd
}

func (self *CatDFSDir) List() DFSEntryList {
	var l DFSEntryList
	for _, fe := range self.children {
		l = append(l, fe)
	}
	return l
}

func (self *CatDFSDir) Status() *proc.CatFileStatus {
	status := &proc.CatFileStatus{
		Filename: self.filename,
		IsDir:    true,
	}
	return status
}
