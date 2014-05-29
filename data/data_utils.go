package data

import (
	"bufio"
	proc "github.com/proj-223/CatFs/protocols"
	"os"
)

func (self *DataServer) prepareNext(param *proc.PrepareBlockParam) (chan []byte, error) {
	nextParam := param.NextPipeParam()
	if nextParam == nil {
		// if there is no more replicas
		return nil, nil
	}
	// prepare next data server
	var nextLease proc.CatLease
	nextDataServer := nextParam.BlockLocation().DataServer(self.pool)
	err := nextDataServer.PrepareSendBlock(nextParam, &nextLease)
	if err != nil {
		return nil, err
	}
	// prepare deliverChan block to next data server
	nextBlockClient := nextParam.BlockLocation().BlockClient(self.pool)
	deliverChan := make(chan []byte)
	go nextBlockClient.SendBlock(deliverChan, nextLease.ID)
	return deliverChan, nil
}

// go routine to receive data
func (self *DataServer) writeBlockToDisk(data chan []byte, block *proc.CatBlock) {
	// TODO get file name
	filename := "/tmp/catfs-test/" + block.ID
	fi, err := os.Open(filename)
	if err != nil {
		// IF error, TODO sth
		return
	}
	defer fi.Close()
	writer := bufio.NewWriter(fi)
	for {
		b, ok := <-data
		if !ok {
			// finish writing
			writer.Flush()
			break
		}
		if b == nil {
			// TODO failed writing
			break
		}
		writer.Write(b)
	}
}
