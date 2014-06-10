package master

import (
	//"fmt"
	proc "github.com/proj-223/CatFs/protocols"
)

type CFSFile struct {
	File_map  map[string]*CFSFile
	IsDir     bool
	Blocklist []string
	Lease_map map[string]*proc.CatFileLease
	Length    int64
}

func (self *CFSFile) GetFile(relativepath []string) (*CFSFile, bool) {
	firstchild, ok := self.File_map[relativepath[0]]
	if !ok {
		return nil, false
	} else if len(relativepath) == 1 {
		return firstchild, true
	} else {
		return firstchild.GetFile(relativepath[1:])
	}
}

func (self *CFSFile) AddFile(relativepath []string, isDirectory bool) error {
	var isDir bool
	if len(relativepath) > 1 {
		isDir = true
	} else {
		isDir = isDirectory
	}

	//first check whether the first element in path is present
	//if not present, then create one
	//fmt.Println(relativepath)
	//fmt.Println(self == nil)
	//fmt.Println(self.File_map == nil)
	//fmt.Println(relativepath[0])
	firstchild, ok := self.File_map[relativepath[0]]
	var directory *CFSFile
	if !ok {
		directory = new(CFSFile)
		directory.File_map = make(map[string]*CFSFile)
		directory.IsDir = isDir
		directory.Blocklist = make([]string, 0)
		directory.Lease_map = make(map[string]*proc.CatFileLease)
		directory.Length = 0
		self.File_map[relativepath[0]] = directory
		//fmt.Println("directory null?", directory == nil)
	} else {
		//fmt.Println("directory null?", directory == nil)
		directory = firstchild
	}
	//fmt.Println("directory null?", directory == nil)
	if len(relativepath) > 1 {
		//fmt.Println("directory null?", directory == nil)
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

func (self *CFSFile) MountFile(relativepath []string, filetomount *CFSFile) {
	length := len(relativepath)
	parentpath := relativepath[:length-1]
	leaf_file := relativepath[length-1]
	var file *CFSFile
	ok := false
	if len(parentpath) > 0 {
		file, ok = self.GetFile(parentpath)
	} else {
		file = self
		ok = true
	}
	if ok {
		file.File_map[leaf_file] = filetomount
	} else {
		panic("Cannot MountFile, the relativepath does not exist!")
	}
}

func (self *CFSFile) DeleteFile(path []string) bool {
	if len(path) == 0 {
		return true
	}
	if len(path) > 1 {
		file, ok := self.File_map[path[0]]
		if !ok {
			return false
		} else {
			return file.DeleteFile(path[1:])
		}
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

func (self *CFSFile) isDirectory() bool {
	return self.IsDir
}
