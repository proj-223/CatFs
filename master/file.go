package master

const (
	DFS_FILETYPE_FILE DFSFileType = iota
	DFS_FILETYPE_DIR
)

type DFSFileType int

type DFSEntry interface {

	Delete() error

	Parent() DFSEntry

	IsDir() bool

	RenameTo(dir DFSEntry, filename string) error

	Filename() string

}

type DFSDir interface {

	DFSEntry

	New(filename string, filetype DFSFileType, mode int) (DFSEntry, error)

	UnMountChild(child DFSEntry) error

	// get file in the dir, return error when it is not dir or
	// there is no such file
	GetFile(filename string) (DFSEntry, error)

}

type CatDFSFile struct {
	filename string
	parent DFSEntry
}

func (self *CatDFSFile) GetFile(filename string) (DFSEntry, error) {
	return nil, ErrNotDir
}

func (self *CatDFSFile) Delete() error {
	panic("to do")
}

func (self *CatDFSFile) Parent() (DFSEntry, error) {
	return self.parent, nil
}

func (self *CatDFSFile) New(filename string, filetype DFSFileType, mode int) (DFSEntry, error) {
	return nil, ErrNotDir
}

func (self *CatDFSFile) IsDir() bool {
	return false
}

func (self *CatDFSFile) RenameTo(dir DFSEntry, filename string) error {
	panic("to do")
}

func (self *CatDFSFile) Filename() string {
	return self.filename
}

func (self *CatDFSFile) UnMountChild(child DFSEntry) error {
	return ErrNotDir
}

type CatDFSDir struct {
	filename string
	parent DFSEntry
	children map[string]DFSEntry
}

func (self *CatDFSDir) GetFile(filename string) (DFSEntry, error) {
	fi, ok := self.children[filename]
	if !ok {
		return nil, ErrNoSuchFile
	}
	return fi, nil
}

func (self *CatDFSDir) Delete() error {
	for _, fi := range self.children {
		fi.Delete()
	}
	// TODO Delete Self
	panic("to do")
}

func (self *CatDFSDir) Parent() DFSEntry {
	return self.parent
}

func (self *CatDFSDir) New(filename string, filetype DFSFileType, mode int) (DFSEntry, error) {
	panic("to do")
}

func (self *CatDFSDir) IsDir() bool {
	return true
}

func (self *CatDFSDir) RenameTo(dir DFSEntry, filename string) error {
	panic("to do")
}

func (self *CatDFSDir) Filename() string {
	return self.filename
}

func (self *CatDFSDir) UnMountChild(child DFSEntry) error {
	panic("to do")
}

func RootDir() DFSDir {
	return nil
}
