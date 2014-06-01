package master
import ( 
	"sync"
	//"fmt"
)

type GFSFile struct {
     File_map map[string]*GFSFile
     IsDir bool
     Blocklist []string
     Lock sync.RWMutex
     Length int64
}

/*func (self* GFSFile) ContainsSingle(filename string) bool {
	_, ok := self.File_map[filename]
	return ok
}

func (self* GFSFile) Contains(relativepath []string) bool {
	firstchild, ok := self.File_map[relativepath[0]]
	if(!ok){
		return false
	} else if(len(relativepath)==1) {
		return true
	} else {
		return firstchild.Contains(relativepath[1:])
	}
}

func (self* GFSFile) GetFileSingle(filename string) *GFSFile {
	file, _ := self.File_map[filename]
	return file
}*/

func (self* GFSFile) GetFile(relativepath []string) (*GFSFile, bool) {
	firstchild, ok := self.File_map[relativepath[0]]
	if(!ok){
		return nil, false
	} else if(len(relativepath)==1) {
		return firstchild, true
	} else {
		return firstchild.GetFile(relativepath[1:])
	}
}

func (self* GFSFile) AddFile(relativepath []string, isDirectory bool) {
	var isDir bool
	if(len(relativepath) > 1){
		isDir = true
	} else {
		isDir = isDirectory
	}

	//first check whether the first element in path is present
	//if not present, then create one
	firstchild, ok := self.File_map[relativepath[0]]
	var directory *GFSFile
	if(!ok) {
		directory := new(GFSFile)
		directory.File_map = make(map[string]*GFSFile)
		directory.IsDir = isDir
	} else {
		directory = firstchild;
	}
	self.File_map[relativepath[0]] = directory
	if(len(relativepath) > 1){
		directory.AddFile(relativepath[1:], isDirectory)
	}
}

func (self* GFSFile) MountFile(relativepath []string, filetomount *GFSFile){
	length := len(relativepath)
	parentpath := relativepath[:length-1]
	leaf_file := relativepath[length-1]
	file, ok := self.GetFile(parentpath)
	if(ok) {
		file.File_map[leaf_file] = filetomount
	} else {
		panic("Cannot MountFile, the relativepath does not exist!")
	}
}

func (self *GFSFile) DeleteFile(path []string) bool {
	if(len(path) == 0) {
		return true
	}
	if(len(path) > 1){
		return self.DeleteFile(path[1:])
	}
	name := path[0]
	_, ok := self.File_map[name]
	if(!ok){
		return false
	} else {
		delete(self.File_map, name)
		return true
	}
}

func (self* GFSFile) isDirectory() bool {
	return self.IsDir
}