package client

import (
	"os"
)

type CatFile struct {
	// TODO
}

// type io.Closer
// Close closes the File, rendering it unusable for I/O. It returns an error, if
// any.
func (self *CatFile) Close() error {
	panic("to do")
}

// IsExist returns a boolean indicating whether a file
// or directory already exists.
func (self *CatFile) IsExist() bool {
	panic("to do")
}

// IsDir returns a boolean indicating whether a file
// is a directory
func (self *CatFile) IsDir() bool {
	panic("to do")
}

// Read reads up to len(b) bytes from the File. It returns the number of bytes read
// and an error, if any. EOF is signaled by a zero count with err set to io.EOF.
func (self *CatFile) Read(b []byte) (n int, err error) {
	panic("to do")
}

// ReadAt reads len(b) bytes from the File starting at byte offset off. It
// returns the number of bytes read and the error, if any. ReadAt always returns
// a non-nil error when n < len(b). At end of file, that error is io.EOF.
func (self *CatFile) ReadAt(b []byte, off int64) (n int, err error) {
	panic("to do")
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
	panic("to do")
}
