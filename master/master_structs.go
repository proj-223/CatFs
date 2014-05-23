package master
import "sync"

type GFSFile struct {
     file_map map[string]GFSFile
     isDir bool
     blocklist []uint64
     Lock sync.RWMutex
}

func (self* GFSFile) Contains(filename string) bool {
	_, ok := self.file_map[filename]
	return ok
}

func (self* GFSFile) GetFile(filename string) GFSFile {
	return self.file_map[filename]
}

func (self* GFSFile) AddFile(path []string, isDirectory bool) {
	var isDir bool
	if(len(path) > 1){
		isDir = true
	} else {
		isDir = isDirectory
	}
	directory := GFSFile{file_map : make(map[string]GFSFile), 
					     isDir: isDir}
	self.file_map[path[0]] = directory
	if(len(path) > 1){
		directory.AddFile(path[1:], isDirectory)
	}
}

func (self* GFSFile) isDirectory() bool {
	return self.isDir
}