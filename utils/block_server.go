package utils

import (
	"errors"
	"github.com/proj-223/CatFs/config"
	"github.com/proj-223/CatFs/protocols"
	"log"
	"net"
)

const (
	Block_Server_START_MSG = "CatFS Data Block Server are start: %s\n"
)

const (
	BLOCK_BUFFER_SIZE  = 1 << 10
	BLOCK_REQUEST_SIZE = 37
)

const (
	REQUEST_SEND_BLOCK = iota
	REQUEST_GET_BLOCK
)

var (
	RESPONSE_PELEASE_SEND = []byte("please send")

	ErrShutdown = errors.New("Operation Error")
)

type BlockRequest struct {
	TransID     string // It is a UUID
	RequestType byte   // It is an int
}

func BlockRequestFromByte(b []byte) *BlockRequest {
	return &BlockRequest{
		TransID:     string(b[:BLOCK_REQUEST_SIZE-1]),
		RequestType: b[BLOCK_REQUEST_SIZE-1],
	}
}

type BlockServer struct {
	transactions map[string]chan []byte
	blockSize    int64
	conf         *config.BlockServerConfig
}

// Start by DataNode
// It will start an go routine waiting for block request
func (self *BlockServer) Serve() error {
	listener, err := net.Listen("tcp", ":"+self.conf.Port)
	if err != nil {
		return err
	}
	log.Printf(Block_Server_START_MSG, self.conf.Port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accept: %s", err.Error())
			continue
		}
		go self.handleRequest(conn)
	}
	return ErrShutdown
}

// DataServer will receive an transaction request and it will call this
// method to add an entry for the transaction
func (self *BlockServer) StartTransaction(lease *protocols.CatLease, c chan []byte) {

}

// Stop one transaction, it may because the transaction terminated or
// the lease is out of date
func (self *BlockServer) StopTransaction(lease *protocols.CatLease) {
	// delete the transaction from map
	delete(self.transactions, lease.ID)
}

func (self *BlockServer) handleRequest(conn net.Conn) {
	requestBuf := make([]byte, BLOCK_REQUEST_SIZE)
	n, err := conn.Read(requestBuf)
	if err != nil {
		// Log the error and return
		log.Printf("Error read: %s", err.Error())
		conn.Close()
		return
	}
	// n should be BLOCK_REQUEST_SIZE
	if n != BLOCK_REQUEST_SIZE {
		log.Printf("Error request size %d != %d", n, BLOCK_REQUEST_SIZE)
		conn.Close()
		return
	}

	req := BlockRequestFromByte(requestBuf)
	switch int(req.RequestType) {
	case REQUEST_SEND_BLOCK:
		// if the request is to send a block to server
		self.handleSendRequest(conn, req.TransID)
	case REQUEST_GET_BLOCK:
		// if the request is get a block from server
		self.handleGetRequest(conn, req.TransID)
	}
}

func (self *BlockServer) handleSendRequest(conn net.Conn, transID string) {
	_, err := conn.Write(RESPONSE_PELEASE_SEND)
	if err != nil {
		// Log the error the return
		log.Printf("Error read: %s", err.Error())
		return
	}

	var dataReceived int64 = 0
	buf := make([]byte, BLOCK_BUFFER_SIZE)
	for dataReceived < self.blockSize {
		n, err := conn.Read(buf)
		if err != nil {
			// Log the error
			log.Printf("Error read: %s", err.Error())
			// tell the channel there is error
			// send nil to channel
			self.transactions[transID] <- nil
			return
		}
		// TODO Question potential racing condition
		// Make the chan limited to 1
		self.transactions[transID] <- buf
		dataReceived += int64(n)
	}
}

func (self *BlockServer) handleGetRequest(conn net.Conn, transID string) {
	var dataSent int64 = 0
	// TODO Question potential racing condition
	// Make the chan limited to 1
	for dataSent < self.blockSize {
		buf, ok := <-self.transactions[transID]
		if !ok {
			break
		}
		dataSent += int64(len(buf))
		// ignore the error
		conn.Write(buf)
	}
}

func NewBlockServer(conf *config.BlockServerConfig) *BlockServer {
	return &BlockServer{
		conf:         conf,
		blockSize:    conf.BlockSize,
		transactions: make(map[string]chan []byte),
	}
}
