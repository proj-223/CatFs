package master

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"sync"
)

var (
	blockManager = &BlockManager{
		blocks: make(map[string]*Block),
		locker: new(sync.Mutex),
	}
)

type Block struct {
	block *proc.CatBlock
	file  *CatDFSFile
}

func (self *Block) ID() string {
	return self.block.ID
}

func (self *Block) Locations() []proc.ServerLocation {
	return self.block.Locations
}

// try to add a replica, return success or not
func (self *Block) AddReplica(loc proc.ServerLocation) bool {
	index := self.findIndex(loc)
	if index != -1 {
		// not add but return true
		return true
	}
	if len(self.block.Locations) >= config.ReplicaCount() {
		// will not add
		return false
	}
	self.block.Locations = append(self.block.Locations, loc)
	return true
}

func (self *Block) CatBlock() *proc.CatBlock {
	return self.block
}

func (self *Block) Migrate(notLoc proc.ServerLocation) {
	i := self.findIndex(notLoc)
	if i == -1 {
		return
	}
	self.block.Locations = append(self.block.Locations[:i], self.block.Locations[i+1:]...)
	replias := slaveManager.NewBlockReplica()
	for _, replia := range replias {
		index := self.findIndex(replia)
		if index == -1 {
			// May Couse bug
			// TODO TODO better algorithm
			slaveManager.AppendMigration(self.block.Locations[0], replia, self.ID())
			return
		}
	}
}

func (self *Block) findIndex(loc proc.ServerLocation) int {
	for i, l := range self.block.Locations {
		if l == loc {
			return i
		}
	}
	return -1
}

type BlockList []*Block

func (self BlockList) ToCatBlock() []*proc.CatBlock {
	var catBlocks []*proc.CatBlock
	for _, b := range self {
		catBlocks = append(catBlocks, b.block)
	}
	return catBlocks
}

type BlockManager struct {
	// key: id, value: block
	blocks map[string]*Block
	locker sync.Locker
}

func (self *BlockManager) GetBlock(id string) *Block {
	if block, ok := self.blocks[id]; ok {
		return block
	}
	return nil
}

func (self *BlockManager) Remove(id string) {
	self.locker.Lock()
	defer self.locker.Unlock()
	if _, ok := self.blocks[id]; ok {
		delete(self.blocks, id)
	}
}

func (self *BlockManager) Register(block *Block) {
	self.locker.Lock()
	defer self.locker.Unlock()
	self.blocks[block.ID()] = block
}

func (self *BlockManager) MigrateBlock(id string, notLoc proc.ServerLocation) {
	if block, ok := self.blocks[id]; ok {
		block.Migrate(notLoc)
	}
}

func (self *BlockManager) New(fi *CatDFSFile) *Block {
	block := &Block{
		block: &proc.CatBlock{
			ID:        uuid.New(),
			Locations: slaveManager.NewBlockReplica(),
		},
		file: fi,
	}
	return block
}
