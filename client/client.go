package client

import (
	ms "github.com/proj-223/CatFs/master"
	proc "github.com/proj-223/CatFs/protocols"
	"github.com/proj-223/CatFs/protocols/pool"
	"os"
)

const (
	OPEN_MODE_READ = iota
	OPEN_MODE_WRITE
)

type CatClient struct {
	pool   *pool.ClientPool
	curdir string // current working directory
}

// IsDir returns a boolean indicating whether a file
// is a directory
func (self *CatClient) IsDir(name string) (bool, error) {
	filestatus, err := self.getFilestatus(name)
	if err != nil {
		return false, err
	}
	return filestatus.IsDir, nil
}

// Chdir changes the current working directory to the named directory. If there
// is an error, it will be of type *PathError.
// The default working dir is "/"
func (self *CatClient) Chdir(dir string) error {
	abspath := Abs(self.curdir, dir)
	isDir, err := self.IsDir(abspath)
	if err != nil {
		return err
	}
	if !isDir {
		return ErrInvalidParam
	}
	self.curdir = abspath
	return nil
}

// Chmod changes the mode of the named file to mode. If the file is a symbolic
// link, it changes the mode of the link's target. If there is an error, it will
// be of type *PathError.
// Default file mode is 644
// Default dir mode is 755
func (self *CatClient) Chmod(name string, mode os.FileMode) error {
	panic("to do")
}

// IsExist returns a boolean indicating whether a file
// or directory already exists.
func (self *CatClient) IsExist(name string) (bool, error) {
	_, err := self.getFilestatus(name)
	if err == nil {
		return true, nil
	}
	if err == ms.ErrNoSuchFile {
		return false, nil
	}
	return false, err
}

// Mkdir creates a new directory with the specified name and permission bits. If
// there is an error, it will be of type *PathError.
func (self *CatClient) Mkdir(name string, perm os.FileMode) error {
	// TODO
	return self.MkdirAll(name, perm)
}

// MkdirAll creates a directory named path, along with any necessary parents, and
// returns nil, or else returns an error. The permission bits perm are used for
// all directories that MkdirAll creates. If path is already a directory,
// MkdirAll does nothing and returns nil.
func (self *CatClient) MkdirAll(name string, perm os.FileMode) error {
	abspath := Abs(self.curdir, name)

	param := &proc.MkdirParam{
		Path: abspath,
	}
	master := self.pool.MasterServer()
	var succ bool
	err := master.Mkdirs(param, &succ)
	return err
}

// Remove removes the named file or directory. If there is an error, it will be
// of type *PathError.
func (self *CatClient) Remove(name string) error {
	// abspath := Abs(self.curdir, name)
	// TODO
	// if it is dir
	// if there is content in the dir
	return self.RemoveAll(name)
}

// RemoveAll removes path and any children it contains. It removes everything it
// can but returns the first error it encounters. If the path does not exist,
// RemoveAll returns nil (no error).
func (self *CatClient) RemoveAll(path string) error {
	abspath := Abs(self.curdir, path)
	master := self.pool.MasterServer()

	param := &proc.DeleteParam{
		Path: abspath,
	}
	var succ bool
	err := master.Delete(param, &succ)
	return err
}

// Rename renames a file.
func (self *CatClient) Rename(oldname, newname string) error {
	param := &proc.RenameParam{
		Src: Abs(self.curdir, oldname),
		Des: Abs(self.curdir, newname),
	}
	master := self.pool.MasterServer()
	var succ bool
	err := master.Rename(param, &succ)
	return err
}

// Close all connection
func (self *CatClient) Close() error {
	self.pool.Close()
	return nil
}

func (self *CatClient) Open(name string, mode int) (*CatFile, error) {
	file := self.GetFile(name)
	err := file.Open(mode)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (self *CatClient) Create(name string) (*CatFile, error) {
	file := self.GetFile(name)
	err := file.Create()
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (self *CatClient) GetFile(name string) *CatFile {
	path := Abs(self.curdir, name)
	return &CatFile{
		path:   path,
		pool:   self.pool,
		opened: false,
	}
}

func (self *CatClient) getFilestatus(name string) (*proc.CatFileStatus, error) {
	abspath := Abs(self.curdir, name)
	master := self.pool.MasterServer()

	var filestatus proc.CatFileStatus
	err := master.GetFileInfo(abspath, &filestatus)
	if err != nil {
		return nil, err
	}
	return &filestatus, nil
}

func (self *CatClient) ListDir(path string) ([]string, error) {
	abspath := Abs(self.curdir, path)
	master := self.pool.MasterServer()
	param := &proc.ListDirParam{
		Path: abspath,
	}
	var resp proc.ListDirResponse
	err := master.Listdir(param, &resp)
	if err != nil {
		return nil, err
	}
	var dircontent []string
	for _, file := range resp.Files {
		dircontent = append(dircontent, file.Filename)
	}
	return dircontent, nil
}

// get current dir
func (self *CatClient) CurrentDir() string {
	return self.curdir
}
