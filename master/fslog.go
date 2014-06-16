package master

import (
	"io"
)

type Log interface {
	Execute(fs *CatFileSystem)
	// save to disk or remote
	Save(writer io.Writer)
	// read from dis or remote
	Load(reader io.Reader)
}

// delete a file or dir
type DeleteLog struct {
	abspath string
}

func (self *DeleteLog) Execute(fs *CatFileSystem) {
	fs.DeleteFile(self.abspath)
}

func (self *DeleteLog) Save(writer io.Writer) {
	panic("to do")
}

func (self *DeleteLog) Load(reader io.Reader) {
	panic("to do")
}

// rename a file or dir
type RenameLog struct {
	src string
	dst string
}

func (self *RenameLog) Execute(fs *CatFileSystem) {
	fs.Rename(self.src, self.dst)
}

func (self *RenameLog) Save(writer io.Writer) {
	panic("to do")
}

func (self *RenameLog) Load(reader io.Reader) {
	panic("to do")
}

// create a file
type CreateFileLog struct {
	abspath string
	mode    int
}

func (self *CreateFileLog) Execute(fs *CatFileSystem) {
	fs.CreateFile(self.abspath, self.mode)
}

func (self *CreateFileLog) Save(writer io.Writer) {
	panic("to do")
}

func (self *CreateFileLog) Load(reader io.Reader) {
	panic("to do")
}

// create a directory
type MkdirsLog struct {
	abspath string
	mode    int
}

func (self *MkdirsLog) Execute(fs *CatFileSystem) {
	fs.Mkdirs(self.abspath, self.mode)
}

func (self *MkdirsLog) Save(writer io.Writer) {
	panic("to do")
}

func (self *MkdirsLog) Load(reader io.Reader) {
	panic("to do")
}
