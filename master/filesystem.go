package master

import (
	"path"
)

type CatFileSystem struct {
	root DFSDir
}

// use cache in the future
func (self *CatFileSystem) GetFile(abspath string) (DFSEntry, error) {
	if abspath == "/" {
		return self.root, nil
	}
	dirname, filename := path.Split(abspath)
	fi, err := self.GetFile(dirname)
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
	file, err := self.GetFile(abspath)
	if err != nil {
		return err
	}
	return file.Delete()
}

func (self *CatFileSystem) CreateFile(abspath string, mode int) (DFSEntry, error) {
	dirname, filename := path.Split(abspath)
	fi, err := self.GetFile(dirname)
	if err != nil {
		return nil, err
	}
	if dir, ok := fi.(DFSDir); ok {
		return dir.New(filename, DFS_FILETYPE_FILE, mode)
	}
	return nil, ErrNotDir
}

func (self *CatFileSystem) Rename(src, dst string) error {
	srcfi, err := self.GetFile(src)
	if err != nil {
		return err
	}
	dstdirname, filename := path.Split(dst)
	dstParentFi, err := self.GetFile(dstdirname)
	if err != nil {
		return err
	}
	dstParentDir, ok := dstParentFi.(DFSDir)
	if !ok {
		return ErrNotDir
	}
	dstfi, err := self.GetFile(filename)
	if err == ErrNoSuchFile {
		return srcfi.RenameTo(dstParentDir, filename)
	}
	if dstDir, ok := dstfi.(DFSDir); ok {
		return srcfi.RenameTo(dstDir, srcfi.Filename())
	}
	return ErrFileAlreadyExist
}

func (self *CatFileSystem) Mkdirs(abspath string, mode int) (DFSEntry, error) {
	dirname, filename := path.Split(abspath)
	fi, err := self.GetFile(dirname)
	if err != nil && err != ErrNoSuchFile {
		fi, err = self.Mkdirs(dirname, mode)
		if err != nil {
			return nil, err
		}
	}
	if dir, ok := fi.(DFSDir); ok {
		return dir.New(filename, DFS_FILETYPE_DIR, mode)
	}
	return nil, ErrNotDir
}

func (self *CatFileSystem) IsExist(abspath string) bool {
	fi, err := self.GetFile(abspath)
	return fi != nil && err != ErrNoSuchFile
}

func NewCatFileSystem() *CatFileSystem {
	fs := &CatFileSystem{
		root: rootDir,
	}
	return fs
}
