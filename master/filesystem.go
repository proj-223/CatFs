package master

import (
	proc "github.com/proj-223/CatFs/protocols"
	"path"
)

var (
	catFileSystem = NewCatFileSystem()
)

type CatFileSystem struct {
	root DFSDir
}

func (self *CatFileSystem) QueryBlocks(abspath string, offset, length int64) (BlockList, bool, error) {
	fi, err := self.GetFile(abspath)
	if err != nil {
		return nil, false, err
	}
	bs, eof := fi.QueryBlocks(offset, length)
	return bs, eof, nil
}

func (self *CatFileSystem) GetFile(abspath string) (DFSFile, error) {
	file, err := self.GetFileEntry(abspath)
	if err != nil {
		return nil, err
	}
	if file.IsDir() {
		return nil, ErrNotFile
	}
	return file.(DFSFile), nil
}

func (self *CatFileSystem) GetDir(abspath string) (DFSDir, error) {
	file, err := self.GetFileEntry(abspath)
	if err != nil {
		return nil, err
	}
	if !file.IsDir() {
		return nil, ErrNotDir
	}
	return file.(DFSDir), nil
}

// use cache in the future
func (self *CatFileSystem) GetFileEntry(abspath string) (DFSEntry, error) {
	if abspath == "/" {
		return self.root, nil
	}
	dirname := path.Dir(abspath)
	filename := path.Base(abspath)
	fi, err := self.GetFileEntry(dirname)
	if err != nil {
		return nil, err
	}
	if dir, ok := fi.(DFSDir); ok {
		return dir.GetFile(filename)
	}
	return nil, ErrNotDir
}

// return nil if there is no error
func (self *CatFileSystem) DeleteFile(abspath string) error {
	file, err := self.GetFileEntry(abspath)
	if err != nil {
		return err
	}
	return file.Delete()
}

func (self *CatFileSystem) CreateFile(abspath string, mode int) (DFSFile, error) {
	dirname := path.Dir(abspath)
	filename := path.Base(abspath)
	fi, err := self.GetFileEntry(dirname)
	if err != nil {
		return nil, err
	}
	if dir, ok := fi.(DFSDir); ok {
		fe, err := dir.New(filename, DFS_FILETYPE_FILE, mode)
		if err != nil {
			return nil, err
		}
		return fe.(DFSFile), nil
	}
	return nil, ErrNotDir
}

func (self *CatFileSystem) Rename(src, dst string) error {
	srcfi, err := self.GetFileEntry(src)
	if err != nil {
		return err
	}
	dstdirname := path.Dir(dst)
	filename := path.Base(dst)
	dstParentFi, err := self.GetFileEntry(dstdirname)
	if err != nil {
		return err
	}
	dstParentDir, ok := dstParentFi.(DFSDir)
	if !ok {
		return ErrNotDir
	}
	dstfi, err := dstParentDir.GetFile(filename)
	if err == ErrNoSuchFile {
		return srcfi.RenameTo(dstParentDir, filename)
	}
	if dstDir, ok := dstfi.(DFSDir); ok {
		return srcfi.RenameTo(dstDir, srcfi.Filename())
	}
	return ErrFileAlreadyExist
}

func (self *CatFileSystem) Mkdirs(abspath string, mode int) (DFSDir, error) {
	dirname := path.Dir(abspath)
	filename := path.Base(abspath)
	fd, err := self.GetDir(dirname)
	if err != nil && err != ErrNoSuchFile {
		return nil, err
	}
	if err == ErrNoSuchFile {
		fd, err = self.Mkdirs(dirname, mode)
		if err != nil {
			return nil, err
		}
	}
	fe, err := fd.New(filename, DFS_FILETYPE_DIR, mode)
	if err != nil {
		return nil, err
	}
	return fe.(DFSDir), nil
}

func (self *CatFileSystem) ListDir(abspath string) (DFSEntryList, error) {
	fd, err := self.GetDir(abspath)
	if err != nil {
		return nil, err
	}
	return fd.List(), nil
}

func (self *CatFileSystem) IsExist(abspath string) bool {
	fi, err := self.GetFileEntry(abspath)
	return fi != nil && err != ErrNoSuchFile
}

func (self *CatFileSystem) AddBlock(abspath string) (*Block, error) {
	fi, err := self.GetFile(abspath)
	if err != nil {
		return nil, err
	}
	return fi.AddBlock()
}

func NewCatFileSystem() *CatFileSystem {
	fs := &CatFileSystem{
		root: rootDir,
	}
	return fs
}

type DFSEntryList []DFSEntry

func (self DFSEntryList) Status() []*proc.CatFileStatus {
	var status []*proc.CatFileStatus
	for _, s := range self {
		status = append(status, s.Status())
	}
	return status
}
