package master

import (
	"io"
)

var (
	logManager = new(LogManager)
)

type LogManager struct {
	diskWriter io.Writer
	// TODO will it work, or use RPC call
	remoteWriter io.Writer
}

func (self *LogManager) CreateFile(abspath string, mode int) {
	log := &CreateFileLog{
		abspath: abspath,
		mode:    mode,
	}
	go log.Save(self.diskWriter)
	// TODO save to remote
}

func (self *LogManager) Mkdirs(abspath string, mode int) {
	log := &MkdirsLog{
		abspath: abspath,
		mode:    mode,
	}
	go log.Save(self.diskWriter)
	// TODO save to remote
}

func (self *LogManager) Rename(src string, dst string) {
	log := &RenameLog{
		src: src,
		dst: dst,
	}
	go log.Save(self.diskWriter)
}

func (self *LogManager) DeleteFile(abspath string) {
	log := &DeleteLog{
		abspath: abspath,
	}
	go log.Save(self.diskWriter)
}

func (self *LogManager) LoadLogs() {
	// TODO load from disk or remote
	// will it work?
	panic("to do")
}
