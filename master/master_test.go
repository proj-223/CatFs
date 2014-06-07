package master

import (
	"fmt"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"runtime/debug"
	"sync"
	"testing"
	"time"
)

const BLOCK_SIZE = 1 << 20
const HEARTBEAT_INTERVAL = time.Second * 10

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

func createMaster() *Master {
	myroot := &GFSFile{
		File_map:  make(map[string]*GFSFile),
		IsDir:     true,
		Blocklist: make([]string, 0),
		Lease_map: make(map[string]*proc.CatFileLease),
		Length:    0,
	}

	lockmanager := &LockManager{
		Lockmap: make(map[string]*sync.Mutex),
	}
	server_addr_list := []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
		"localhost:8083",
		"localhost:8084",
	}
	server_livemap := []bool{
		true, true, true, true, true,
	}

	master := &Master{
		conf:     config.DefaultMachineConfig,
		root:     *myroot,
		blockmap: make(map[string]*proc.CatBlock),
		//mapping from LeaseID to CatFileLease and GFSFile
		master_lease_map: make(map[string]*FileLease),
		dataserver_addr:  server_addr_list,
		livemap:          server_livemap,
		lockmgr:          *lockmanager,
		StatusList:       make(map[proc.ServerLocation]*ServerStatus),
		CommandList:      make(map[proc.ServerLocation]chan *proc.MasterCommand),
	}

	return master
}

func TestBasics(t *testing.T) {
	master := createMaster()
	//register data servers
	var succ bool
	StatusList := make([]*proc.DataServerStatus, 0)
	for i := 0; i < 5; i++ {
		status := &proc.DataServerStatus{
			Location:     (proc.ServerLocation)(i),
			AvaiableSize: 0,
			DataSize:     0,
			TotalSize:    0,
			Errors:       make([]string, 0),
			BlockReports: make(map[string]*proc.DataBlockReport),
		}

		master.RegisterDataServer(&proc.RegisterDataParam{Status: status}, &succ)

		StatusList = append(StatusList, status)
		fmt.Println(StatusList)
		as(succ, t)
	}
	as(master.StatusList != nil, t)

	custompath := "/helloworld.txt"
	custompath2 := "/folder1/haohuan.bmp"
	custompath3 := "/folder1/haohuan2.bmp"
	//first create a file
	createFileparam := &proc.CreateFileParam{Path: custompath}
	createPicFileparam := &proc.CreateFileParam{Path: custompath2}
	createPicFileparam2 := &proc.CreateFileParam{Path: custompath3}
	createFileResponse := &proc.OpenFileResponse{Filestatus: &proc.CatFileStatus{}, Lease: &proc.CatFileLease{}}
	master.Create(createFileparam, createFileResponse)
	as(createFileResponse.Lease != nil, t)
	as(createFileResponse.Filestatus.Length == 0, t)

	master.Create(createPicFileparam, createFileResponse)
	as(createFileResponse.Filestatus.Filename == "haohuan.bmp", t)
	as(createFileResponse.Filestatus.Length == 0, t)

	e := master.Create(createPicFileparam2, createFileResponse)
	ne(e, t)
	as(createFileResponse.Filestatus.Filename == "haohuan2.bmp", t)
	as(createFileResponse.Filestatus.Length == 0, t)

	//then open the file, should be no errors
	openFileparam := &proc.OpenFileParam{Path: custompath}
	fakeFileparam := &proc.OpenFileParam{Path: "/helloworld2.txt"}

	e = master.Open(openFileparam, createFileResponse)
	ne(e, t)

	e = master.Open(fakeFileparam, createFileResponse)
	er(e, t)

	//add a block to each of the three files
	paths := []string{custompath, custompath2, custompath3}
	for i := 0; i < 3; i++ {
		addblockparam := &proc.AddBlockParam{Path: paths[i], Lease: &proc.CatFileLease{}}
		catblock := &proc.CatBlock{}
		e = master.AddBlock(addblockparam, catblock)
		ne(e, t)
		//fmt.Println(catblock.ID)
		//fmt.Println(catblock.Locations)
		as(len(catblock.ID) > 0 && catblock.Locations != nil, t)
	}

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

func TestMigration(t *testing.T) {
	master := createMaster()
	//register data servers
	var succ bool
	StatusList := make([]*proc.DataServerStatus, 0)
	for i := 0; i < 5; i++ {
		status := &proc.DataServerStatus{
			Location:     (proc.ServerLocation)(i),
			AvaiableSize: 0,
			DataSize:     0,
			TotalSize:    0,
			Errors:       make([]string, 0),
			BlockReports: make(map[string]*proc.DataBlockReport)}

		master.RegisterDataServer(&proc.RegisterDataParam{Status: status}, &succ)

		StatusList = append(StatusList, status)
		fmt.Println(StatusList)
		as(succ, t)
	}

	//create a bunch of files and add block to these files
	count := 10
	createFileparam := &proc.CreateFileParam{}
	createFileResponse := &proc.OpenFileResponse{Filestatus: &proc.CatFileStatus{}, Lease: &proc.CatFileLease{}}
	addblockparam := &proc.AddBlockParam{Lease: &proc.CatFileLease{}}
	catblock := &proc.CatBlock{}
	var pathname string
	var e error
	for i := 0; i < count; i++ {
		pathname = "/file" + (string)(i) + ".txt"
		createFileparam.Path = pathname
		e = master.Create(createFileparam, createFileResponse)
		as(e == nil, t)
		addblockparam.Path = pathname
		e = master.AddBlock(addblockparam, catblock)
		fmt.Println(catblock.ID)
		fmt.Println(catblock.Locations)
		as(e == nil, t)
	}

	master.StartMonitor()
	time.Sleep(HEARTBEAT_INTERVAL * 2 / 3)
	alive_server_idx := []int{0, 1, 2, 3}
	heartbeat := &proc.HeartbeatParam{}
	response := &proc.HeartbeatResponse{}
	for i := 0; i < len(alive_server_idx); i++ {
		loc := alive_server_idx[i]
		heartbeat.Status = StatusList[loc]
		master.SendHeartbeat(heartbeat, response)
	}
	time.Sleep(HEARTBEAT_INTERVAL * 2 / 3)

	as(len(master.CommandList) > 0, t)

	for k, v := range master.CommandList {
		//fmt.Println("key: ", k, v)
		for flag := true; flag; {
			select {
			case Cmd := <-v:
				println("retrieve cmd: copy blocks ", Cmd.Blocks[0], " from ", k, " to ", Cmd.DstMachine)
			default:
				//println("No command")
				flag = false
			}
		}
	}
}
