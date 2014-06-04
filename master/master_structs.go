package master

import (
	//"fmt"
	proc "github.com/proj-223/CatFs/protocols"
)

type GFSFile struct {
	File_map  map[string]*GFSFile
	IsDir     bool
	Blocklist []string
	Lease_map map[string]*proc.CatFileLease
	Length    int64
}

func (self *GFSFile) GetFile(relativepath []string) (*GFSFile, bool) {
	firstchild, ok := self.File_map[relativepath[0]]
	if !ok {
		return nil, false
	} else if len(relativepath) == 1 {
		return firstchild, true
	} else {
		return firstchild.GetFile(relativepath[1:])
	}
}

func (self *GFSFile) AddFile(relativepath []string, isDirectory bool) error {
	var isDir bool
	if len(relativepath) > 1 {
		isDir = true
	} else {
		isDir = isDirectory
	}

	//first check whether the first element in path is present
	//if not present, then create one
	firstchild, ok := self.File_map[relativepath[0]]
	var directory *GFSFile
	if !ok {
		directory := new(GFSFile)
		directory.File_map = make(map[string]*GFSFile)
		directory.IsDir = isDir
		directory.Blocklist = make([]string, 0)
		directory.Lease_map = make(map[string]*proc.CatFileLease)
		directory.Length = 0
		self.File_map[relativepath[0]] = directory
	} else {
		directory = firstchild
	}
	if len(relativepath) > 1 {
		return directory.AddFile(relativepath[1:], isDirectory)
	} else {
		if ok {
			//it means the file to create already exists
			return ErrFileAlreadyExist
		} else {
			return nil
		}
	}
}

func (self *GFSFile) MountFile(relativepath []string, filetomount *GFSFile) {
	length := len(relativepath)
	parentpath := relativepath[:length-1]
	leaf_file := relativepath[length-1]
	file, ok := self.GetFile(parentpath)
	if ok {
		file.File_map[leaf_file] = filetomount
	} else {
		panic("Cannot MountFile, the relativepath does not exist!")
	}
}

func (self *GFSFile) DeleteFile(path []string) bool {
	if len(path) == 0 {
		return true
	}
	if len(path) > 1 {
		return self.DeleteFile(path[1:])
	}
	name := path[0]
	_, ok := self.File_map[name]
	if !ok {
		return false
	} else {
		delete(self.File_map, name)
		return true
	}
}

func (self *GFSFile) isDirectory() bool {
	return self.IsDir
}
