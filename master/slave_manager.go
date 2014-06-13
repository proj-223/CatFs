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
		locations = append(locations, slave.Location())
	}
	return locations
}

// register a slave
func (self *SlaveManager) RegisterSlave(status *proc.DataServerStatus) {
	slave := &Slave{
		locker: new(sync.Mutex),
	}
	slave.Update(status)
	self.locker.Lock()
	defer self.locker.Unlock()
	self.slaves[slave.Location()] = slave
}

// update a slave
// receive a heartbeat from dataserver
func (self *SlaveManager) UpdateSlave(status *proc.DataServerStatus) {
	if slave, ok := self.slaves[status.Location]; ok {
		slave.Update(status)
		return
	}
	self.RegisterSlave(status)
}
