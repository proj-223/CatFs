package master

import (
	"container/heap"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
)

var (
	slaveManager = &SlaveManager{
		slaves: make(map[proc.ServerLocation]*Slave),
	}
)

type Slave struct {
	status *proc.DataServerStatus
	// the id of blocks
	blocks map[string]bool
}

func (self *Slave) Location() proc.ServerLocation {
	return self.status.Location
}

type SlaveHeap []*Slave

func (h SlaveHeap) Len() int { return len(h) }

func (h SlaveHeap) Less(i, j int) bool { return len(h[i].blocks) < len(h[j].blocks) }

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
}

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

func (self *SlaveManager) RegisterBlockReplica(block *Block) {
	locations := block.Locations()
	for _, location := range locations {
		slave := self.slaves[location]
		slave.blocks[block.ID()] = true
	}
}

func (self *SlaveManager) RemoveBlock(block *Block) {
	locations := block.Locations()
	for _, location := range locations {
		slave := self.slaves[location]
		delete(slave.blocks, block.ID())
	}
}

func (self *SlaveManager) RegisterSlave(status *proc.DataServerStatus) {
	slave := &Slave{
		status: status,
	}
	for blockId := range status.BlockReports {
		slave.blocks[blockId] = true
	}
	self.slaves[slave.Location()] = slave
}
