package master

import (
	"errors"
	"time"
)

const (
	START_MSG      = "CatFS Master RPC are start: %s"
	HEARTBEAT_TICK = 5 * time.Second
	CHANNEL_SIZE   = 100
)

var (
	ErrNoSuchFile           = errors.New("No such file")
	ErrParentDirNotExist    = errors.New("parent dir not exist")
	ErrFileAlreadyExist     = errors.New("The file already exists")
	ErrNotEnoughAliveServer = errors.New("Not enough alive servers")
	ErrNotDir               = errors.New("Not a dir")
	ErrNotFile              = errors.New("Not a file")
	ErrIsRoot               = errors.New("Is Root")
	ErrUnKnownFileType      = errors.New("Unknown file type")
	ErrBadRequest           = errors.New("Bad Request")
)

func Serve() error {
	done := make(chan error, 1)

	go initMasterRPC(done)
	go slaveManager.Exam()
	err := <-done

	return err
}
