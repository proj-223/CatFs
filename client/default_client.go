package client

import (
	"github.com/proj-223/CatFs/config"
	"os"
)

var (
	DefaultCatClient *CatClient = NewCatClient(config.DefaultMachineConfig)
)

// Chdir changes the current working directory to the named directory. If there
// is an error, it will be of type *PathError.
// The default working dir is "/"
func Chdir(dir string) error {
	return DefaultCatClient.Chdir(dir)
}

// Chmod changes the mode of the named file to mode. If the file is a symbolic
// link, it changes the mode of the link's target. If there is an error, it will
// be of type *PathError.
// Default file mode is 644
// Default dir mode is 755
func Chmod(name string, mode os.FileMode) error {
	return DefaultCatClient.Chmod(name, mode)
}

// IsExist returns a boolean indicating whether a file
// or directory already exists.
func IsExist(name string) (bool, error) {
	return DefaultCatClient.IsExist(name)
}

// IsDir returns a boolean indicating whether a file
// is a directory
func IsDir(name string) (bool, error) {
	return DefaultCatClient.IsDir(name)
}

// Mkdir creates a new directory with the specified name and permission bits. If
// there is an error, it will be of type *PathError.
func Mkdir(name string, perm os.FileMode) error {
	return DefaultCatClient.Mkdir(name, perm)
}

// MkdirAll creates a directory named path, along with any necessary parents, and
// returns nil, or else returns an error. The permission bits perm are used for
// all directories that MkdirAll creates. If path is already a directory,
// MkdirAll does nothing and returns nil.
func MkdirAll(name string, perm os.FileMode) error {
	return DefaultCatClient.MkdirAll(name, perm)
}

// Remove removes the named file or directory. If there is an error, it will be
// of type *PathError.
func Remove(name string) error {
	return DefaultCatClient.Remove(name)
}

// RemoveAll removes path and any children it contains. It removes everything it
// can but returns the first error it encounters. If the path does not exist,
// RemoveAll returns nil (no error).
func RemoveAll(path string) error {
	return DefaultCatClient.RemoveAll(path)
}

// Rename renames a file.
func Rename(oldname, newname string) error {
	return DefaultCatClient.Rename(oldname, newname)
}
