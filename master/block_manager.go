package master

import (
	"code.google.com/p/go-uuid/uuid"
	proc "github.com/proj-223/CatFs/protocols"
)

var (
	blockManager = &BlockManager{
		blocks: make(map[string]*Block),
	}
)

type Block struct {
	block *proc.CatBlock
	file  *CatDFSFile
}

func (self *Block) Register() {
	slaveManager.RegisterBlockReplica(self)
}

func (self *Block) ID() string {
	return self.block.ID
}

func (self *Block) Locations() []proc.ServerLocation {
	return self.block.Locations
}

type BlockManager struct {
	// key: id, value: block
	blocks map[string]*Block
}

func (self *BlockManager) GetBlock(id string) *Block {
	if block, ok := self.blocks[id]; ok {
		return block
	}
	return nil
}

func (self *BlockManager) Remove(id string) {
	if _, ok := self.blocks[id]; ok {
		delete(self.blocks, id)
	}
}

func (self *BlockManager) New(fi *CatDFSFile) *Block {
	// TODO get the location
	block := &Block{
		block: &proc.CatBlock{
			ID:        uuid.New(),
			Locations: slaveManager.NewBlockReplica(),
		},
		file: fi,
	}
	return block
}
