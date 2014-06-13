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
	status     *proc.DataServerStatus
	lastUpdate time.Time
	locker     sync.Locker
	commands   chan *proc.MasterCommand
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

func (self *Slave) AppendCommand(cmd *proc.MasterCommand) {
	go func() {
		self.commands <- cmd
	}()
}

func (self *Slave) Migrate() {
	for _, block := range self.status.BlockReports {
		go blockManager.MigrateBlock(block.ID, self.Location())
	}
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

func (self *Slave) cleanBlock(id string) {
	cmd := &proc.MasterCommand{
		Command: proc.CleanCommand,
		Blocks:  []string{id},
	}
	self.AppendCommand(cmd)
}

func (self *Slave) GetCommands() []*proc.MasterCommand {
	var commands []*proc.MasterCommand
	for {
		select {
		case cmd := <-self.commands:
			commands = append(commands, cmd)
		default:
			return commands
		}
	}
	return nil
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
		locker:   new(sync.Mutex),
		commands: make(chan *proc.MasterCommand, CHANNEL_SIZE),
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

func (self *SlaveManager) AppendCommand(loc proc.ServerLocation, cmd *proc.MasterCommand) {
	slave := self.slaves[loc]
	slave.AppendCommand(cmd)
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
