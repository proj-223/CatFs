package master

import (
	"fmt"
	"testing"
	"sync"
	//"os"
	"runtime/debug"
	//"time"
	proc "github.com/proj-223/CatFs/protocols"
)

func TestExample(t *testing.T) {
	fmt.Println("Hello, World!")
}

func ne(e error, t *testing.T) {
	if e != nil {
		debug.PrintStack()
		t.Fatal(e)
	}
}

func er(e error, t *testing.T) {
	if e == nil {
		debug.PrintStack()
		t.Fatal()
	}
}

func as(cond bool, t *testing.T) {
	if !cond {
		debug.PrintStack()
		t.Fatal()
	}
}

func TestMaster(t *testing.T) {
	myroot := &GFSFile{File_map: make(map[string]*GFSFile),
	IsDir:true,
	Blocklist: make([]string, 0),
	Lease_map: make(map[string]*proc.CatFileLease),
	Length: 0}

	lockmanager := &LockManager{Lockmap: make(map[string]*sync.Mutex)} 

	master := &Master{root: *myroot, 
			  blockmap: make(map[string]*proc.CatBlock),
			  //mapping from LeaseID to CatFileLease and GFSFile
			  master_lease_map: make(map[string]*FileLease),
	          dataserver_addr: make([]string, 0),
	          livemap: make([]bool, 0),
			  lockmgr: *lockmanager}

	custompath := "/helloworld.txt"
	//first create a file
	createFileparam := &proc.CreateFileParam{Path: custompath}
	createFileResponse := &proc.OpenFileResponse{Filestatus: &proc.CatFileStatus{}, Lease: &proc.CatFileLease{}}
	master.Create(createFileparam, createFileResponse)
	as(createFileResponse.Lease!=nil, t)

	//then open the file, should be no errors
	openFileparam := &proc.OpenFileParam{Path: custompath}
	fakeFileparam := &proc.OpenFileParam{Path: "/helloworld2.txt"}

	e := master.Open(openFileparam, createFileResponse)
	ne(e, t)

	e = master.Open(fakeFileparam, createFileResponse)
	er(e, t)

	//add a block to the file
	addblockparam := &proc.AddBlockParam{Path: custompath, Lease: &proc.CatFileLease{}}
	catblock := &proc.CatBlock{}
	e = master.AddBlock(addblockparam, catblock)

	ne(e, t)
	as(len(catblock.ID) == 0 && catblock.Locations != nil, t)




}