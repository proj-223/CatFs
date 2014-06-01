package client

import (
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"os"
)

type CatClient struct {
	pool *proc.ClientPool
	conf *config.MachineConfig
}

// Chdir changes the current working directory to the named directory. If there
// is an error, it will be of type *PathError.
// The default working dir is "/"
func (self *CatClient) Chdir(dir string) error {
	panic("to do")
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
func (self *CatClient) IsExist(name string) bool {
	panic("to do")
}

// IsDir returns a boolean indicating whether a file
// is a directory
func (self *CatClient) IsDir(name string) bool {
	panic("to do")
}

// Mkdir creates a new directory with the specified name and permission bits. If
// there is an error, it will be of type *PathError.
func (self *CatClient) Mkdir(name string, perm os.FileMode) error {
	panic("to do")
}

// MkdirAll creates a directory named path, along with any necessary parents, and
// returns nil, or else returns an error. The permission bits perm are used for
// all directories that MkdirAll creates. If path is already a directory,
// MkdirAll does nothing and returns nil.
func (self *CatClient) MkdirAll(name string, perm os.FileMode) error {
	panic("to do")
}

// Remove removes the named file or directory. If there is an error, it will be
// of type *PathError.
func (self *CatClient) Remove(name string) error {
	panic("to do")
}

// RemoveAll removes path and any children it contains. It removes everything it
// can but returns the first error it encounters. If the path does not exist,
// RemoveAll returns nil (no error).
func (self *CatClient) RemoveAll(path string) error {
	panic("to do")
}

// Rename renames a file.
func (self *CatClient) Rename(oldname, newname string) error {
	panic("to do")
}

// Close all connection
func (self *CatClient) Close() error {
	panic("to do")
}

// Open a file for read
func (self *CatClient) Open(name string, mode int) (file *CatFile, err error) {
	panic("to do")
}
