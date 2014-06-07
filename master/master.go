package master

import (
	uuid "code.google.com/p/go-uuid/uuid"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"fmt"
)

const REPLICA_COUNT = 3
const BLOCK_SIZE = 1024
const HEARTBEAT_INTERVAL = time.Second
const CHANNEL_SIZE = 100

type FileLease struct {
	Lease *proc.CatFileLease
	File  *GFSFile
}

type ServerStatus struct {
	LastUpdate time.Time
	Status *proc.DataServerStatus
}

//a mapping from migration destination to the blocks
//that are to be migrated to that destination 
/*type CommandList struct {
	CmdList []*proc.MasterCommand
}*/

type Master struct {
	root GFSFile
	//mapping from blockID to block location
	blockmap map[string]*proc.CatBlock
	//mapping from LeaseID to CatFileLease and GFSFile
	master_lease_map map[string]*FileLease
	//the address of servers can be used, it may contain 
	//servers that are currently down
	dataserver_addr  []string
	//the key is the address of data server
	livemap          []bool
	conf             *config.MachineConfig
	lockmgr          LockManager
	StatusList map[proc.ServerLocation]*ServerStatus
	//it stores for each data server the commands to
	//be executed
	CommandList map[proc.ServerLocation]chan *proc.MasterCommand
}

// Get location of the block of the specified file within the specified range
func (self *Master) GetBlockLocation(query *proc.BlockQueryParam, blocks *proc.GetBlocksLocationResponse) error {
	elements := PathToElements(query.Path)
	file, ok := self.root.GetFile(elements)
	if !ok {
		return ErrNoSuchFile
	}
	start_idx := (int)(query.Offset / BLOCK_SIZE)
	end_idx := (int)((query.Offset + query.Length) / BLOCK_SIZE)
	block_count := len(file.Blocklist)
	if end_idx >= block_count-1 {
		end_idx = block_count - 1
		blocks.EOF = true
	} else {
		blocks.EOF = false
	}

	blocks.Blocks = make([]*proc.CatBlock, 0)
	for i := start_idx; i <= end_idx; i++ {
		ID := file.Blocklist[i]
		blocks.Blocks = append(blocks.Blocks, self.blockmap[ID])
	}

	return nil
}

func (self *Master) _get_replicas(path string, replica *proc.CatBlock) error {
	hash := fnv.New32a()
	hash.Write([]byte(path))
	hash_int := hash.Sum32()
	//fmt.Println("hash_int: ", hash_int)
	i := 0
	server_num := len(self.dataserver_addr)
	//fmt.Println("server_num: ",server_num)
	replica.Locations = make([]proc.BlockLocation, 0)
	for len(replica.Locations) < REPLICA_COUNT {
		if i == len(self.livemap) {
			return ErrNotEnoughAliveServer
		}
		idx := (proc.BlockLocation)((int(hash_int) + i) % server_num)
		if idx < 0 {
			idx = idx + (proc.BlockLocation)(server_num)
		}
		//key := self.dataserver_addr[int(idx)]
		if self.livemap[idx] {
			replica.Locations = append(replica.Locations, idx)
		}
		i++
	}

	replica.ID = uuid.New()
	return nil
}

//delete the data blocks associated with the
//file/directory
func (self *Master) _clear_file(file *GFSFile) {
	//If it is a file, then just delete the data blocks
	//Can be optimized using go routine
	for _, blockId := range file.Blocklist {
		delete(self.blockmap, blockId)
	}

	for name, child := range file.File_map {
		self._clear_file(child)
		delete(file.File_map, name)
	}
}

func (self *Master) _clear_expire_lease() {
	current_time := time.Now()
	for k, v := range self.master_lease_map {
		//Delete expired leases
		if v.Lease.Expire.Before(current_time) {
			file := v.File
			//delete the lease from the lease map of the file
			delete(file.Lease_map, k)
			//delete the lease from the global map of the master
			delete(self.master_lease_map, k)
		}
	}
}

func (self *Master) _load_command() {
	fmt.Println("Check liveness begin")
	current_time := time.Now()
	for addr, v := range self.StatusList {
		//the server is down
		println(addr, v.LastUpdate.String())
		if ( current_time.Sub(v.LastUpdate) > HEARTBEAT_INTERVAL ) {
			self.livemap[addr] = false
		}
	}

	//fmt.Println(self.livemap)
	fmt.Println(len(self.blockmap))
	for ID, block := range self.blockmap {
		fmt.Println("ID: ",ID, " ,Location: ",block.Locations)
		//live serverlist
		//var down_server proc.BlockLocation
		var down_server_idx int
		var max_location proc.BlockLocation
		max_location = (proc.BlockLocation)(0)

		needsMigration := false
		for i,location := range block.Locations {
			if(!self.livemap[(int)(location)]) {
				//down_server = location
				down_server_idx = i
				needsMigration = true
			}
			if(location > max_location) {
				max_location = location
			}
		}

		if(!needsMigration) {
			continue
		}

		//fmt.Println("ID: ",ID, " ,Location: ",block.Locations)
		//find the backup server for the down server, can I
		//make it any faster?
		j := max_location+1
		p := 0
		for p<len(self.livemap) {
			if((int)(j) >= len(self.livemap)) {
				j = j - (proc.BlockLocation)(len(self.livemap))
			}

			//it must be alive
			if(!self.livemap[j]) {
				p++
				j++
				continue
			}

			//and it must not be in the 
			//set of already existing replicas
			isIn := false
			for _,location := range block.Locations {
				if((int)(location) == (int)(j)) {
					isIn = true
					break
				}
			}

			if(!isIn) {
				break
			}
			p++
			j++
			//println(p)
		}

		backup := (proc.ServerLocation)(j)
		//println("find backup: ",backup)
		//locate the source, i.e. the previous one 
		//of the down server
		t := (down_server_idx-1)
		if( t <0) {
			t += REPLICA_COUNT
		}
		//fmt.Println("Detect dead server", block.Locations[down_server_idx], " at block ", ID)
		//fmt.Println("all replicas: ", block.Locations)
		//create commands
		source_loc := (proc.ServerLocation)(block.Locations[t])	
		Cmd := &proc.MasterCommand{Command: proc.MigrationCommand, Blocks: []string{ID}, DstMachine: backup}
		_, ok := self.CommandList[source_loc]
		if(!ok) {
			self.CommandList[source_loc] = make(chan *proc.MasterCommand, CHANNEL_SIZE)
		}	
		//fmt.Println("Add command", source_loc)
		go func() {
			self.CommandList[source_loc] <- Cmd
		}()
		//fmt.Println("ID: 333",ID, " ,Location: ",block.Locations)

	}


}

func (self *Master) StartMonitor() {
	monitor := func() {
		for {
			//fmt.Println("start monitor")
			self._load_command()
			time.Sleep(HEARTBEAT_INTERVAL)
		}
	}
	go monitor()
}

// Create a file in a given path
func (self *Master) Create(param *proc.CreateFileParam, response *proc.OpenFileResponse) error {
	//panic("to do")
	self.lockmgr.AcquireLock(param.Path)
	elements := PathToElements(param.Path)
	e := self.root.AddFile(elements, false)
	if e != nil {
		return e
	}
	self.lockmgr.ReleaseLock(param.Path)
	fs_state := response.Filestatus
	fs_state.Filename = elements[len(elements)-1]
	fs_state.Length = 0
	current_time := time.Now()
	fs_state.CTime = current_time
	fs_state.MTime = current_time
	fs_state.ATime = current_time
	fs_state.IsDir = false
	response.Lease.ID = uuid.New()
	response.Lease.Type = proc.LEASE_WRITE
	response.Lease.Expire = time.Now()
	response.Lease.Expire.Add(proc.LEASE_DURATION)

	//put the lease into the lease_map of the file
	file, ok := self.root.GetFile(elements)
	if !ok {
		return ErrNotEnoughAliveServer
	} else {
		file.Lease_map[response.Lease.ID] = response.Lease
		self.master_lease_map[response.Lease.ID] = &FileLease{Lease: response.Lease, File: file}
	}
	return nil
}

// Open a file to add block
func (self *Master) Open(param *proc.OpenFileParam, response *proc.OpenFileResponse) error {
	//First locate the GFSFile instance
	elements := PathToElements(param.Path)
	file, ok := self.root.GetFile(elements)

	if !ok {
		return ErrNoSuchFile
	}

	fs_state := response.Filestatus
	fs_state.Filename = param.Path
	fs_state.Length = file.Length
	current_time := time.Now()
	fs_state.CTime = current_time
	fs_state.MTime = current_time
	fs_state.ATime = current_time
	fs_state.IsDir = false
	response.Lease.ID = uuid.New()
	response.Lease.Type = proc.LEASE_WRITE
	response.Lease.Expire = time.Now()
	response.Lease.Expire.Add(proc.LEASE_DURATION)

	//What if the file gets deleted before this line is executed?
	file.Lease_map[response.Lease.ID] = response.Lease
	self.master_lease_map[response.Lease.ID] = &FileLease{Lease: response.Lease, File: file}
	return nil
}

// Delete a block from a file
func (self *Master) AbandonBlock(param *proc.AbandonBlockParam, succ *bool) error {
	//panic("to do")
	elements := PathToElements(param.Path)
	self.lockmgr.AcquireLock(param.Path)
	file, ok := self.root.GetFile(elements)
	if !ok {
		return ErrNoSuchFile
	}
	blockId := param.Block.ID
	delete(self.blockmap, blockId)
	//delete from Blocklist, could not think of a better
	//algorithm that can work better than linear time
	for i, v := range file.Blocklist {
		if v == blockId {
			file.Blocklist = append(file.Blocklist[:i], file.Blocklist[i+1:]...)
			break
		}
	}
	self.lockmgr.ReleaseLock(param.Path)
	return nil
}

// Add a block to a specific path (file)
func (self *Master) AddBlock(param *proc.AddBlockParam, block *proc.CatBlock) error {
	elements := PathToElements(param.Path)
	file, ok := self.root.GetFile(elements)
	if !ok {
		return ErrNoSuchFile
	}
	e := self._get_replicas(param.Path, block)
	if e != nil {
		return e
	}
	file.Blocklist = append(file.Blocklist, block.ID)
	file.Length = file.Length + BLOCK_SIZE
	//copy a new one from the input block
	self.blockmap[block.ID] = &proc.CatBlock{ID: block.ID, Locations: block.Locations}
	return nil
}

// Complete an operation,
// delete the lease (lock)
func (self *Master) Close(param *proc.CloseParam, succ *bool) error {
	//panic("to do")
	elements := PathToElements(param.Path)
	file, ok := self.root.GetFile(elements)
	if !ok {
		return ErrNoSuchFile
	} else {
		delete(file.Lease_map, param.Lease.ID)
		delete(self.master_lease_map, param.Lease.ID)
	}
	return nil
}

// Rename
func (self *Master) Rename(param *proc.RenameParam, succ *bool) error {
	//It is basically delete then create
	src_elements := PathToElements(param.Src)
	dst_elements := PathToElements(param.Des)

	file, ok := self.root.GetFile(src_elements)
	if !ok {
		return ErrNoSuchFile
	}
	if !self.root.DeleteFile(src_elements) {
		return ErrNoSuchFile
	}
	self.root.MountFile(dst_elements, file)
	*succ = true
	return nil
}

// Delete a file
func (self *Master) Delete(param *proc.DeleteParam, succ *bool) error {
	elements := PathToElements(param.Path)
	file, ok := self.root.GetFile(elements)
	if !ok {
		return ErrNoSuchFile
	}

	//First remove the meta data
	if !self.root.DeleteFile(elements) {
		*succ = false
		return ErrNoSuchFile
	} else {
		*succ = true
	}

	//Then delete the data blocks
	self._clear_file(file)
	return nil
}

// Create a dir
func (self *Master) Mkdirs(param *proc.MkdirParam, succ *bool) error {
	elements := PathToElements(param.Path)
	self.root.AddFile(elements, true)
	*succ = true
	return nil
}

// List dir, why the return value is not a list?
func (self *Master) Listdir(param *proc.ListDirParam, response *proc.ListDirResponse) error {
	elements := PathToElements(param.Path)
	//fmt.Println(elements, len(elements))
	var file *GFSFile
	if len(elements) > 0 {
		var ok bool
		file, ok = self.root.GetFile(elements)
		if !ok {
			return ErrNoSuchFile
		}
	} else {
		//fmt.Println(elements)
		file = &self.root
	}

	response.Files = make([]*proc.CatFileStatus, 0)

	//var file_status_list []*proc.CatFileStatus
	for k, v := range file.File_map {
		file_status := new(proc.CatFileStatus)
		file_status.Filename = k
		file_status.Length = v.Length
		response.Files = append(response.Files, file_status)
	}
	return nil
}

// Renew a lease
func (self *Master) RenewLease(oldLease *proc.CatFileLease, newLease *proc.CatFileLease) error {
	_, ok := self.master_lease_map[oldLease.ID]
	if !ok {
		panic("The lease is longer valid")
	}
	self.master_lease_map[oldLease.ID].Lease.Expire.Add(proc.LEASE_DURATION)
	return nil
}

// File info
func (self *Master) GetFileInfo(path string, filestatus *proc.CatFileStatus) error {
	//panic("to do")
	elements := PathToElements(path)
	file, ok := self.root.GetFile(elements)
	if !ok {
		return ErrNoSuchFile
	} else {
		filestatus.Filename = elements[len(elements)-1]
		filestatus.Length = file.Length
		return nil
	}
}

// Register a data server
func (self *Master) RegisterDataServer(param *proc.RegisterDataParam, succ *bool) error {
	//panic("to do")
	self.StatusList[param.Status.Location] = &ServerStatus{LastUpdate: time.Now(), Status: param.Status}
	self.livemap[(int)(param.Status.Location)] = true
	*succ = true
	return nil
}

// Send heartbeat to master
func (self *Master) SendHeartbeat(param *proc.HeartbeatParam, rep *proc.HeartbeatResponse) error {
	//fmt.Println("send heartbeat null", param.Status == nil )
	st, ok := self.StatusList[param.Status.Location]
	if(!ok) {
		self.StatusList[param.Status.Location] = &ServerStatus{LastUpdate: time.Now(), Status: param.Status}
		st = self.StatusList[param.Status.Location]
	} else {
		st.LastUpdate = time.Now()
	}
	//check whether there are commands pending to be sent
	cmdList := self.CommandList[param.Status.Location]

	for flag := true; flag ; {

		select {
			case Cmd := <- cmdList:
				//println("retrieve cmd", Cmd)
				rep.Command = append(rep.Command, Cmd)
			default: 
				//println("no more command")
				flag= false
		}
	}

	return nil
}

// Send blockreport to master
func (self *Master) BlockReport(param *proc.BlockReportParam, rep *proc.BlockReportResponse) error {
	panic("to do")
}

// go routine to init the data rpc server
func (self *Master) initRPCServer(done chan error) {
	server := rpc.NewServer()
	server.Register(proc.MasterProtocol(self))
	l, err := net.Listen("tcp", self.conf.MasterAddr())
	if err != nil {
		done <- err
		return
	}
	log.Printf(START_MSG, self.conf.MasterAddr())
	err = http.Serve(l, server)
	done <- err
}
