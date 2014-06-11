package master

import (
	"path"
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
}

type DFSDir interface {
	DFSEntry

	New(filename string, filetype DFSFileType, mode int) (DFSEntry, error)

	UnMountChild(child DFSEntry)

	MountChild(child DFSEntry) error

	// get file in the dir, return error when it is not dir or
	// there is no such file
	GetFile(filename string) (DFSEntry, error)
}

type CatDFSFile struct {
	filename string
	abspath  string
	parent   DFSDir
	blocks   []*Block
}

func (self *CatDFSFile) GetFile(filename string) (DFSEntry, error) {
	return nil, ErrNotDir
}

func (self *CatDFSFile) Delete() error {
	for _, block := range self.blocks {
		blockManager.Remove(block.ID())
	}
	return nil
}

func (self *CatDFSFile) Parent() DFSDir {
	return self.parent
}

func (self *CatDFSFile) IsDir() bool {
	return false
}

func (self *CatDFSFile) RenameTo(dir DFSDir, filename string) error {
	err := dir.MountChild(self)
	if err != nil {
		return err
	}
	self.parent.UnMountChild(self)
	self.parent = dir
	return nil
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
	return dir.MountChild(self)
}

func (self *CatDFSDir) Filename() string {
	return self.filename
}

func (self *CatDFSDir) UnMountChild(child DFSEntry) {
	if _, ok := self.children[child.Filename()]; ok {
		delete(self.children, child.Filename())
	}
}

func (self *CatDFSDir) MountChild(child DFSEntry) error {
	if _, ok := self.children[child.Filename()]; ok {
		return ErrFileAlreadyExist
	}
	self.children[child.Filename()] = child
	return nil
}

func (self *CatDFSDir) newFile(filename string, mode int) DFSEntry {
	fi := &CatDFSFile{
		filename: filename,
		parent:   self,
		abspath:  path.Join(self.abspath, filename),
	}
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
	}
	return fd
}
