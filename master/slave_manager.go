package master

import (
	"container/heap"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"sync"
	"time"
)

var (
	slaveManager = &SlaveManager{
		slaves: make(map[proc.ServerLocation]*Slave),
		locker: new(sync.Mutex),
	}
)

type Slave struct {
	status            *proc.DataServerStatus
	lastUpdate        time.Time
	locker            sync.Locker
	blocksToClean     []string
	migrationCommands map[proc.ServerLocation]*proc.MasterCommand
}

func (self *Slave) BlockNum() int {
	return len(self.status.BlockReports)
}

func (self *Slave) Location() proc.ServerLocation {
	return self.status.Location
}

func (self *Slave) Update(status *proc.DataServerStatus) {
	self.locker.Lock()
	defer self.locker.Unlock()
	self.lastUpdate = time.Now()
	self.status = status
}

func (self *Slave) IsAlive(cpm time.Time) bool {
	return self.lastUpdate.Add(HEARTBEAT_TICK * 2).After(cpm)
}

func (self *Slave) AppendMigration(dst proc.ServerLocation, blockId string) {
	self.locker.Lock()
	defer self.locker.Unlock()
	if cmd, ok := self.migrationCommands[dst]; ok {
		// if there is an command for dst
		cmd.Blocks = append(cmd.Blocks, blockId)
		return
	}
	cmd := &proc.MasterCommand{
		Command:    proc.MigrationCommand,
		Blocks:     []string{blockId},
		DstMachine: dst,
	}
	self.migrationCommands[dst] = cmd
}

func (self *Slave) Migrate() {
	for _, block := range self.status.BlockReports {
		go blockManager.MigrateBlock(block.ID, self.Location())
	}
}

func (self *Slave) cleanBlock(id string) {
	self.locker.Lock()
	defer self.locker.Unlock()
	self.blocksToClean = append(self.blocksToClean, id)
}

func (self *Slave) ExamBlock() {
	// TODO it is thread safe?
	for _, b := range self.status.BlockReports {
		id := b.ID
		block := blockManager.GetBlock(id)
		if block == nil {
			go self.cleanBlock(id)
			continue
		}
		succ := block.AddReplica(self.Location())
		if !succ {
			go self.cleanBlock(id)
		}
	}
}

func (self *Slave) GetCommands() []*proc.MasterCommand {
	self.locker.Lock()
	defer self.locker.Unlock()

	var commands []*proc.MasterCommand
	if len(self.blocksToClean) > 0 {
		cleanCmd := &proc.MasterCommand{
			Command: proc.CleanCommand,
			Blocks:  self.blocksToClean,
		}
		// set it to nil
		self.blocksToClean = nil
		commands = append(commands, cleanCmd)
	}
	for _, cmd := range self.migrationCommands {
		commands = append(commands, cmd)
	}
	self.migrationCommands = make(map[proc.ServerLocation]*proc.MasterCommand)
	return commands
}

type SlaveHeap []*Slave

func (h SlaveHeap) Len() int { return len(h) }

func (h SlaveHeap) Less(i, j int) bool { return h[i].BlockNum() < h[j].BlockNum() }

func (h SlaveHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *SlaveHeap) Push(x interface{}) { *h = append(*h, x.(*Slave)) }

func (h *SlaveHeap) Pop() interface{} {
	old := *h
	n := len(old)
	last := old[n-1]
	*h = old[:n-1]
	return last
}

type SlaveManager struct {
	slaves map[proc.ServerLocation]*Slave
	locker sync.Locker
}

// compute the location for a new block
func (self *SlaveManager) NewBlockReplica() []proc.ServerLocation {
	h := &SlaveHeap{}
	heap.Init(h)
	for _, slave := range self.slaves {
		heap.Push(h, slave)
	}
	var locations []proc.ServerLocation
	for h.Len() > 0 && len(locations) < config.ReplicaCount() {
		slave := heap.Pop(h).(*Slave)
		if slave.IsAlive(time.Now()) {
			locations = append(locations, slave.Location())
		}
	}
	return locations
}

// register a slave
func (self *SlaveManager) RegisterSlave(status *proc.DataServerStatus) {
	slave := &Slave{
		locker:            new(sync.Mutex),
		migrationCommands: make(map[proc.ServerLocation]*proc.MasterCommand),
	}
	slave.Update(status)
	self.locker.Lock()
	defer self.locker.Unlock()
	self.slaves[slave.Location()] = slave
}

// update a slave
// receive a heartbeat from dataserver
func (self *SlaveManager) UpdateSlave(status *proc.DataServerStatus) []*proc.MasterCommand {
	if slave, ok := self.slaves[status.Location]; ok {
		slave.Update(status)
		return slave.GetCommands()
	}
	self.RegisterSlave(status)
	return nil
}

func (self *SlaveManager) AppendMigration(src, dst proc.ServerLocation, blockId string) {
	slave := self.slaves[src]
	slave.AppendMigration(dst, blockId)
}

func (self *SlaveManager) Exam() {
	// TODO verify
	c := time.Tick(HEARTBEAT_TICK * 3)
	for _ = range c {
		println("tick")
		go self.examSlaveAliveRoutine()
		go self.examSlaveBlockRoutine()
	}
}

func (self *SlaveManager) examSlaveAliveRoutine() {
	now := time.Now()
	for _, slave := range self.slaves {
		if !slave.IsAlive(now) {
			go slave.Migrate()
		}
	}
}

func (self *SlaveManager) examSlaveBlockRoutine() {
	now := time.Now()
	for _, slave := range self.slaves {
		if slave.IsAlive(now) {
			go slave.ExamBlock()
		}
	}
}
