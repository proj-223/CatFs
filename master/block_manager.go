package master

import (
	"code.google.com/p/go-uuid/uuid"
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

func (self *Block) CatBlock() *proc.CatBlock {
	return self.block
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
