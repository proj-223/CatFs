package master

import (
	"errors"
	"github.com/proj-223/CatFs/config"
	"sync"

	proc "github.com/proj-223/CatFs/protocols"
)

const (
	START_MSG = "CatFS Master RPC are start: %s"
)

var (
	ErrNoSuchFile           = errors.New("No such file")
	ErrFileAlreadyExist     = errors.New("The file already exists")
	ErrNotEnoughAliveServer = errors.New("Not enough alive servers")
)

var (
	DefaultMaster = NewMasterServer(config.DefaultMachineConfig)
)

func Serve() error {
	return ServeMaster(DefaultMaster)
}

// Init the Master Server
func ServeMaster(master *Master) error {
	done := make(chan error, 1)

	// init the rpc server
	go master.initRPCServer(done)

	err := <-done
	return err
}

// Create a new Master Server
func NewMasterServer(conf *config.MachineConfig) *Master {
	myroot := &CFSFile{File_map: make(map[string]*CFSFile),
		IsDir:     true,
		Blocklist: make([]string, 0),
		Lease_map: make(map[string]*proc.CatFileLease),
		Length:    0}

	lockmanager := &LockManager{Lockmap: make(map[string]*sync.Mutex)}
	//server_addr_list := []string{"localhost:8080", "localhost:8081", "localhost:8082", "localhost:8083", "localhost:8084"}
	server_livemap := make(map[proc.ServerLocation]bool)

	master := &Master{root: *myroot,
		blockmap: make(map[string]*proc.CatBlock),
		//mapping from LeaseID to CatFileLease and CFSFile
		master_lease_map: make(map[string]*FileLease),
		livemap:          server_livemap,
		lockmgr:          *lockmanager,
		conf:             conf,
		StatusList:       make(map[proc.ServerLocation]*ServerStatus),
		CommandList:      make(map[proc.ServerLocation]chan *proc.MasterCommand)}

	return master
}
