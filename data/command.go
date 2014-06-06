package data

import (
	proc "github.com/proj-223/CatFs/protocols"
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
}

func (self *DataServer) execMigration(command *proc.MasterCommand) {
}
