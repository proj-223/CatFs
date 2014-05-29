package data

import (
	proc "github.com/proj-223/CatFs/protocols"
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
	// prepare direct block to next data server
	nextBlockClient := nextParam.BlockLocation().BlockClient(self.pool)
	direct := make(chan []byte)
	go nextBlockClient.SendBlock(direct, nextLease.ID)
	return direct, nil
}

// go routine to receive data
func (self *DataServer) receiveBlockRoutine(receive, direct chan []byte, block *proc.CatBlock) {
	for {
		b, ok := <-receive
		if !ok {
			close(direct)
			break
		}
		if direct != nil {
			direct <- b
		}
		// TODO write data to disk
	}
}
