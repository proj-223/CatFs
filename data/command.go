package data

import (
	proc "github.com/proj-223/CatFs/protocols"
	"log"
	"os"
)

func (self *DataServer) initCommandHandler() {
	for {
		command := <-self.commands
		switch command.Command {
		case proc.CleanCommand:
			go self.execCleanCommand(command)
		case proc.MigrationCommand:
			go self.execMigration(command)
		}
	}
}

func (self *DataServer) execCleanCommand(command *proc.MasterCommand) {
	for _, block := range command.Blocks {
		filename := self.blockFilenameFromID(block)
		err := os.Remove(filename)
		if err != nil {
			log.Printf("Error clean: %s\n", err.Error())
		}
	}
}

func (self *DataServer) execMigration(command *proc.MasterCommand) {
	location := proc.BlockLocation(command.DstMachine)
	for _, blockStr := range command.Blocks {
		block := &proc.CatBlock{
			ID:        blockStr,
			Locations: []proc.BlockLocation{location},
		}
		go self.migrateBlock(block)
	}
}

func (self *DataServer) migrateBlock(block *proc.CatBlock) {
	prepareParam := &proc.PrepareBlockParam{
		Block: block,
	}
	location := prepareParam.BlockLocation()
	dataserver := self.pool.DataServer(location)
	var lease proc.CatLease
	err := dataserver.PrepareSendBlock(prepareParam, &lease)
	if err != nil {
		log.Printf("Migration Prepare Error: %s\n", err.Error())
		return
	}
	deliverChan := make(chan []byte)
	blockClient := self.pool.NewBlockClient(location)
	go self.readBlockFromDisk(deliverChan, block)
	go blockClient.SendBlock(deliverChan, lease.ID)

	sendingParam := &proc.SendingBlockParam{
		Lease: &lease,
	}
	var succ bool
	err = dataserver.SendingBlock(sendingParam, &succ)
	if !succ {
		err = ErrOperationFailed
	}
	if err != nil {
		log.Printf("Migration Sending Error: %s\n", err.Error())
	}
}
