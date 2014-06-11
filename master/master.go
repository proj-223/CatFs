package master

import (
	uuid "code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	//"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"sort"
)

const CHANNEL_SIZE = 100

type FileLease struct {
	Lease *proc.CatFileLease
	File  *CFSFile
}

type ServerStatus struct {
	LastUpdate time.Time
	Status     *proc.DataServerStatus
}

type ServerList struct {
	L []*proc.DataServerStatus
}

func (s ServerList) Len() int {
	return len(s.L)
}

func (s ServerList) Swap(i, j int)  { 
	s.L[i], s.L[j] = s.L[j], s.L[i] 
} 

func (s ServerList) Less(i,j int) bool {
	return s.L[i].DataSize  < s.L[j].DataSize
}

type Master struct {
	root CFSFile
	//mapping from blockID to block location
	blockmap map[string]*proc.CatBlock
	//mapping from LeaseID to CatFileLease and CFSFile
	master_lease_map map[string]*FileLease
	//the key is ServerLocation
	livemap    map[proc.ServerLocation]bool
	conf       *config.MachineConfig
	lockmgr    LockManager
	StatusList map[proc.ServerLocation]*ServerStatus
	//it stores for each data server the commands to
	//be executed
	CommandList map[proc.ServerLocation]chan *proc.MasterCommand
}

// Get location of the block of the specified file within the specified range
func (self *Master) GetBlockLocation(query *proc.BlockQueryParam, resp *proc.GetBlocksLocationResponse) error {
	elements := PathToElements(query.Path)
	file, ok := self.root.GetFile(elements)
	if !ok {
		return ErrNoSuchFile
	}
	start_idx := (int)(query.Offset / self.conf.BlockSize())
	end_idx := (int)((query.Offset + query.Length - 1) / self.conf.BlockSize())
	block_count := len(file.Blocklist)
	if end_idx >= block_count-1 {
		end_idx = block_count - 1
		resp.EOF = true
	} else {
		resp.EOF = false
	}

	for i := start_idx; i <= end_idx; i++ {
		ID := file.Blocklist[i]
		resp.Blocks = append(resp.Blocks, self.blockmap[ID])
	}
	return nil
}

func (self *Master) getReplicas(path string, replica *proc.CatBlock) error {
	//fmt.Println("server_num: ",server_num)
	replica.Locations = make([]proc.ServerLocation, 0)
	
	//Naively using sort
	data_status_list := &ServerList{}
	for _, v := range self.StatusList {
		data_status := v.Status
		data_status_list.L  = append(data_status_list.L, data_status)
	}

	if(len(data_status_list.L) < self.conf.ReplicaCount()) {
		return ErrNotEnoughAliveServer
	}

	sort.Sort(data_status_list)
	for _,v := range data_status_list.L[:self.conf.ReplicaCount()] {
		replica.Locations = append(replica.Locations, v.Location)
	}

	replica.ID = uuid.New()
	return nil
}

//delete the data blocks associated with the
//file/directory
func (self *Master) clearFile(file *CFSFile) {
	//If it is a file, then just delete the data blocks
	//Can be optimized using go routine
	for _, blockId := range file.Blocklist {
		delete(self.blockmap, blockId)
	}

	for name, child := range file.File_map {
		self.clearFile(child)
		delete(file.File_map, name)
	}
}

// TODO No one use it ?
func (self *Master) clearExpireLease() {
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

func (self *Master) findSrcBackupServer(servers []proc.ServerLocation) (proc.ServerLocation, proc.ServerLocation) {
	j := servers[len(servers)-1]
	p := 0
	for p < len(self.livemap) {
		if (int)(j) >= len(self.livemap) {
			j = j - (proc.ServerLocation)(len(self.livemap))
		}

		//it must be alive
		if !self.livemap[j] {
			p++
			j++
			continue
		}

		//and it must not be in the
		//set of already existing replicas
		isIn := false
		for _, location := range servers {
			if (int)(location) == (int)(j) {
				isIn = true
				break
			}
		}

		if !isIn {
			break
		}
		p++
		j++
	}

	var src proc.ServerLocation
	for _, v := range servers {
		if self.livemap[v] {
			src = v
			break
		}
	}
	return src, (proc.ServerLocation)(j)
}

func (self *Master) appendCommand(src proc.ServerLocation, cmd *proc.MasterCommand) {
	_, ok := self.CommandList[src]
	if !ok {
		self.CommandList[src] = make(chan *proc.MasterCommand, CHANNEL_SIZE)
	}
	//fmt.Println("Add command", source_loc)
	go func() {
		self.CommandList[src] <- cmd
	}()
}

func (self *Master) loadCommand() {
	fmt.Println("Check liveness begin")
	current_time := time.Now()
	for addr, v := range self.StatusList {
		//println(addr, v.LastUpdate.String())
		if self.livemap[addr] {
			//if the server is down, create migration command
			if current_time.Sub(v.LastUpdate) > self.conf.HeartBeatInterval() {
				self.livemap[addr] = false
				//println("begin add commands for ", addr)
				for ID := range v.Status.BlockReports {
					src, backup := self.findSrcBackupServer(self.blockmap[ID].Locations)
					//println("add command ", src, backup)
					Cmd := &proc.MasterCommand{Command: proc.MigrationCommand, Blocks: []string{ID}, DstMachine: backup}
					self.appendCommand(src, Cmd)
				}
			} else {
				//else create clean command if necessary
				for ID := range v.Status.BlockReports {
					//check whether the current server is in the three replica,
					//if not, clean it
					isIn := false
					for _, loc := range self.blockmap[ID].Locations {
						if loc == addr {
							isIn = true
							break
						}
					}
					if !isIn {
						Cmd := &proc.MasterCommand{Command: proc.CleanCommand, Blocks: []string{ID}, DstMachine: addr}
						self.appendCommand(addr, Cmd)
					}
				}
			}
		}
	}
}

func (self *Master) StartMonitor() {
	monitor := func() {
		for {
			//fmt.Println("start monitor")
			self.loadCommand()
			time.Sleep(self.conf.HeartBeatInterval())
		}
	}
	go monitor()
}

// Create a file in a given path
func (self *Master) Create(param *proc.CreateFileParam, resp *proc.OpenFileResponse) error {
	self.lockmgr.AcquireLock(param.Path)
	elements := PathToElements(param.Path)
	e := self.root.AddFile(elements, false)
	if e != nil {
		return e
	}
	self.lockmgr.ReleaseLock(param.Path)
	current_time := time.Now()

	resp.Filestatus = &proc.CatFileStatus{
		Filename: elements[len(elements)-1],
		Length:   0,
		CTime:    current_time,
		MTime:    current_time,
		ATime:    current_time,
		IsDir:    false,
	}
	resp.Lease = proc.NewFileLease(proc.LEASE_WRITE)

	//put the lease into the lease_map of the file
	file, ok := self.root.GetFile(elements)
	if !ok {
		return ErrNoSuchFile
	}
	file.Lease_map[resp.Lease.ID] = resp.Lease
	self.master_lease_map[resp.Lease.ID] = &FileLease{
		Lease: resp.Lease,
		File:  file,
	}
	return nil
}

// Open a file to add block
func (self *Master) Open(param *proc.OpenFileParam, resp *proc.OpenFileResponse) error {
	//First locate the CFSFile instance
	elements := PathToElements(param.Path)
	file, ok := self.root.GetFile(elements)

	if !ok {
		return ErrNoSuchFile
	}

	// TODO time might be wrong?
	current_time := time.Now()
	resp.Filestatus = &proc.CatFileStatus{
		Filename: elements[len(elements)-1],
		Length:   file.Length,
		CTime:    current_time,
		MTime:    current_time,
		ATime:    current_time,
		IsDir:    false,
	}

	resp.Lease = proc.NewFileLease(proc.LEASE_WRITE)

	//put the lease into the lease_map of the file
	file.Lease_map[resp.Lease.ID] = resp.Lease
	self.master_lease_map[resp.Lease.ID] = &FileLease{
		Lease: resp.Lease,
		File:  file,
	}
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
	e := self.getReplicas(param.Path, block)
	if e != nil {
		return e
	}
	file.Blocklist = append(file.Blocklist, block.ID)
	file.Length = file.Length + self.conf.BlockSize()
	//copy a new one from the input block
	self.blockmap[block.ID] = &proc.CatBlock{
		ID:        block.ID,
		Locations: block.Locations,
	}

	//Add into block report
	for _, loc := range block.Locations {
		block_report := &proc.DataBlockReport{
			ID:     block.ID,
			Status: proc.BLOCK_OK,
		}
		self.StatusList[loc].Status.BlockReports[block.ID] = block_report
		self.StatusList[loc].Status.DataSize = self.StatusList[loc].Status.DataSize + (uint64)(self.conf.BlockSize()) ;
	}
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
	log.Println("Src: ", param.Src, " Dst: ", param.Des)
	src_elements := PathToElements(param.Src)
	dst_elements := PathToElements(param.Des)

	file, ok := self.root.GetFile(src_elements)
	log.Println("Try to get file: ", param.Src)
	if !ok {
		return ErrNoSuchFile
	}
	log.Println("Try to delete file: ", param.Src)
	if !self.root.DeleteFile(src_elements) {
		return ErrNoSuchFile
	}
	log.Println("Try to delete file: ", param.Src)
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
	self.clearFile(file)
	return nil
}

// Create a dir
func (self *Master) Mkdirs(param *proc.MkdirParam, succ *bool) error {
	log.Println("add file ", *succ)
	elements := PathToElements(param.Path)
	e := self.root.AddFile(elements, true)
	if e == nil {
		*succ = true
		log.Println("add file ", *succ)
	} else {
		*succ = false
		log.Println("add file ", *succ)
	}
	return e
}

// List dir, why the return value is not a list?
func (self *Master) Listdir(param *proc.ListDirParam, resp *proc.ListDirResponse) error {
	elements := PathToElements(param.Path)
	//fmt.Println(elements, len(elements))
	var file *CFSFile
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

	resp.Files = nil
	//var file_status_list []*proc.CatFileStatus
	for name, info := range file.File_map {
		fileStatus := &proc.CatFileStatus{
			Filename: name,
			Length:   info.Length,
			IsDir:    info.IsDir,
		}
		resp.Files = append(resp.Files, fileStatus)
	}
	return nil
}

// Renew a lease
func (self *Master) RenewLease(oldLease *proc.CatFileLease, newLease *proc.CatFileLease) error {
	_, ok := self.master_lease_map[oldLease.ID]
	if !ok {
		panic("The lease is longer valid")
	}
	newLease.Renew(oldLease)
	self.master_lease_map[oldLease.ID].Lease = newLease
	return nil
}

// File info
func (self *Master) GetFileInfo(path string, filestatus *proc.CatFileStatus) error {
	//panic("to do")
	elements := PathToElements(path)
	var file *CFSFile
	var ok bool
	if len(elements) > 0 {
		file, ok = self.root.GetFile(elements)
	} else {
		//It is the root
		filestatus.Filename = "/"
		filestatus.Length = self.root.Length
		filestatus.IsDir = self.root.IsDir
		return nil
	}
	if !ok {
		return ErrNoSuchFile
	} else {
		filestatus.Filename = elements[len(elements)-1]
		filestatus.Length = file.Length
		filestatus.IsDir = file.IsDir
		return nil
	}
}

// Register a data server
func (self *Master) RegisterDataServer(param *proc.RegisterDataParam, succ *bool) error {
	self.StatusList[param.Status.Location] = &ServerStatus{LastUpdate: time.Now(), Status: param.Status}
	self.livemap[param.Status.Location] = true
	log.Printf("DataServer %d registered", param.Status.Location)
	*succ = true
	return nil
}

// Send heartbeat to master
func (self *Master) SendHeartbeat(param *proc.HeartbeatParam, rep *proc.HeartbeatResponse) error {
	//fmt.Println("send heartbeat null", param.Status == nil )
	st, ok := self.StatusList[param.Status.Location]
	if !ok {
		self.StatusList[param.Status.Location] = &ServerStatus{
			LastUpdate: time.Now(),
			Status:     param.Status,
		}
		st = self.StatusList[param.Status.Location]
	} else {
		st.LastUpdate = time.Now()
	}
	//check whether there are commands pending to be sent
	cmdList := self.CommandList[param.Status.Location]

	for flag := true; flag; {
		select {
		case Cmd := <-cmdList:
			//println("retrieve cmd", Cmd)
			rep.Command = append(rep.Command, Cmd)
		default:
			//println("no more command")
			flag = false
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
