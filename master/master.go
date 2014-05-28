package master

import (
	"hash/fnv"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

const REPLICA_COUNT = 3

type Replica struct {
	Replica_loc []int 
}

type Master struct {
	root GFSFile
	//mapping from blockID to block location
	blockmap map[uint32]*Replica
	dataserver_addr []string
	livemap []bool	 
	conf *config.MachineConfig
}

// Get location of the block of the specified file within the specified range
func (self *Master) GetBlockLocation(query *proc.BlockQueryParam, blocks *proc.GetBlocksLocationParam) error {
	panic("to do")
}

func (self *Master) _get_replicas(path string, replica *Replica) (uint32, error) {
	hash := fnv.New32a()
	hash.Write([]byte(path))
	hash_int := hash.Sum32()
	i := 0
	server_num := len(self.livemap)
	for len(replica.Replica_loc) < REPLICA_COUNT {
		if i==len(self.livemap) {
			return 0, &NotEnoughAliveServer{}
		}
		idx := (int(hash_int) + i) % server_num
		if(self.livemap[idx]) {
			replica.Replica_loc = append(replica.Replica_loc, idx)
		}
		i++
	}
	return hash_int, nil
}

// Create a file in a given path
func (self *Master) Create(param *proc.CreateFileParam, response *proc.OpenFileResponse) error {
	//panic("to do")
	elements := PathToElements(param.Path)
	self.root.AddFile(elements, false)
	return nil
}

// Open a file to add block
func (self *Master) Open(param *proc.OpenFileParam, response *proc.OpenFileResponse) error {
	//First locate the GFSFile instance
	elements := PathToElements(param.Path)
	file, _ := self.root.GetFile(elements)

	//The try to acquire the lock for that file depending 
	//on the read/write modes
	if(param.Mode == 0){
		file.Lock.RLock()
	} else {
		file.Lock.Lock()
	}

	return nil
}

// Drop a block
func (self *Master) AbandonBlock(param *proc.AbandonBlockParam, succ *bool) error {
	panic("to do")
}

// Add a block to a specific path (file)
func (self *Master) AddBlock(param *proc.AddBlockParam, block *proc.CatBlock) error {
	elements := PathToElements(param.Path)
	file, ok := self.root.GetFile(elements)
	if(!ok){
		return &FileNotExistError{}
	}
	replica := Replica{Replica_loc: make([]int, 0)}
	blockId, e := self._get_replicas(param.Path, &replica)
	if(e!=nil) {
		return e
	}
	file.Blocklist = append(file.Blocklist, blockId)
	self.blockmap[blockId] = &replica

	//why blockID is string?
	//block
	block.Location = replica.Replica_loc 
	return nil
}

// Complete an operation,
// delete the lease (lock)
func (self *Master) Close(param *proc.CloseParam, succ *bool) error {
	panic("to do")
}

// Rename
func (self *Master) Rename(param *proc.RenameParam, succ *bool) error {
	//It is basically delete then create
	src_elements := PathToElements(param.Src)
	dst_elements := PathToElements(param.Des)
	
	file, ok := self.root.GetFile(src_elements)
	if(!ok) {
		return &FileNotExistError{}
	}
	if(!self.root.DeleteFile(src_elements)){
		return &FileNotExistError{}
	}
	self.root.MountFile(dst_elements, file)
	*succ = true
	return nil
}

// Delete a file
func (self *Master) Delete(param *proc.DeleteParam, succ *bool) error {
	//panic("to do")
	//First delete the data blocks
	elements := PathToElements(param.Path)
	file, ok := self.root.GetFile(elements)
	if(!ok){
		return &FileNotExistError{}
	}

	//Can be optimized using go routine
	for _,blockId := range file.Blocklist {
		delete(self.blockmap, blockId)
	}

	//Then remove the leaf file from the parent entry
	if(!self.root.DeleteFile(elements)){
		*succ = false
		return &FileNotExistError{}
	} else {
		*succ = true
		return nil
	}
}

// Create a dir
func (self *Master) Mkdirs(param *proc.MkdirParam, succ *bool) error {
	elements := PathToElements(param.Path)
	self.root.AddFile(elements, true)
	*succ = true
	return nil
}

// List dir, why the return value is not a list?
func (self *Master) Listdir(param *proc.ListDirParam, response *proc.ListdirResponse) error {
	elements := PathToElements(param.Path)
	file, ok := self.root.GetFile(elements)
	if(!ok) {
		return &FileNotExistError{}
	}
	//var file_status_list []*proc.CatFileStatus
	for k,v := range file.File_map {
		file_status := new(proc.CatFileStatus)
		file_status.Filename = k
		file_status.Length = v.Length
		response.Files = append(response.Files, file_status) 
	}
	return nil
}

// Renew a lease
func (self *Master) RenewLease(oldLease *proc.CatFileLease, newLease *proc.CatFileLease) error {
	panic("to do")
}

// File info
func (self *Master) GetFileInfo(path string, filestatus *proc.CatFileStatus) error {
	//panic("to do")
	elements := PathToElements(path)
	file, ok := self.root.GetFile(elements)
	if(!ok) {
		return &FileNotExistError{}
	} else {
		filestatus.Filename = elements[len(elements)-1]
		filestatus.Length = file.Length
		return nil
	}
}

// Register a data server
func (self *Master) RegisterDataServer(param *proc.RegisterDataParam, succ *bool) error {
	panic("to do")
}

// Send heartbeat to master
func (self *Master) SendHeartbeat(param *proc.HeartbeatParam, rep *proc.HeartbeatResponse) error {
	panic("to do")
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
