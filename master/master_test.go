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
	server_addr_list := []string{"localhost:8080", "localhost:8081", "localhost:8082"}
	server_livemap := []bool{true, true, true}
	master := &Master{root: *myroot, 
			  blockmap: make(map[string]*proc.CatBlock),
			  //mapping from LeaseID to CatFileLease and GFSFile
			  master_lease_map: make(map[string]*FileLease),
	          dataserver_addr: server_addr_list,
	          livemap: server_livemap,
			  lockmgr: *lockmanager}

	custompath := "/helloworld.txt"
	custompath2 := "/folder1/haohuan.bmp"
	custompath3 := "/folder1/haohuan2.bmp"
	//first create a file
	createFileparam := &proc.CreateFileParam{Path: custompath}
	createPicFileparam := &proc.CreateFileParam{Path: custompath2}
	createPicFileparam2 := &proc.CreateFileParam{Path: custompath3}
	createFileResponse := &proc.OpenFileResponse{Filestatus: &proc.CatFileStatus{}, Lease: &proc.CatFileLease{}}
	master.Create(createFileparam, createFileResponse)
	as(createFileResponse.Lease!=nil, t)

	master.Create(createPicFileparam, createFileResponse)
	as(createFileResponse.Filestatus.Filename == "haohuan.bmp", t)

	e := master.Create(createPicFileparam2, createFileResponse)
	ne(e, t)
	as(createFileResponse.Filestatus.Filename == "haohuan2.bmp", t)

	//then open the file, should be no errors
	openFileparam := &proc.OpenFileParam{Path: custompath}
	fakeFileparam := &proc.OpenFileParam{Path: "/helloworld2.txt"}

	e = master.Open(openFileparam, createFileResponse)
	ne(e, t)

	e = master.Open(fakeFileparam, createFileResponse)
	er(e, t)

	//add a block to the file
	addblockparam := &proc.AddBlockParam{Path: custompath, Lease: &proc.CatFileLease{}}
	catblock := &proc.CatBlock{}
	e = master.AddBlock(addblockparam, catblock)

	ne(e, t)
	//fmt.Println(catblock.ID)
	//fmt.Println(catblock.Locations)
	as(len(catblock.ID) > 0 && catblock.Locations != nil, t)
	e = master.Open(openFileparam, createFileResponse)
	ne(e, t)
	as(createFileResponse.Filestatus.Length == BLOCK_SIZE, t)

	//test ListDir
	listDirParam := &proc.ListDirParam{Path: "/"}
	listDirParam2 := &proc.ListDirParam{Path: "/folder1"}
	ListDirResponse := &proc.ListDirResponse{Files: make([]*proc.CatFileStatus, 0)}
	e = master.Listdir(listDirParam, ListDirResponse)
	ne(e, t)
	as(len(ListDirResponse.Files) == 2, t)
	as(ListDirResponse.Files[0].Filename == "helloworld.txt", t)
	as(ListDirResponse.Files[1].Filename == "folder1", t)

	e = master.Listdir(listDirParam2, ListDirResponse)
	ne(e, t)
	as(len(ListDirResponse.Files) == 2, t)
	as(ListDirResponse.Files[0].Filename == "haohuan.bmp", t)
	as(ListDirResponse.Files[1].Filename == "haohuan2.bmp", t)	





}