package utils

import (
	"errors"
	"github.com/proj-223/CatFs/config"
	"log"
	"net"
)

const (
	Block_Server_START_MSG = "CatFS Data Block Server are start: %s\n"
)

const (
	BLOCK_BUFFER_SIZE  = 1 << 10
	BLOCK_REQUEST_SIZE = 100
	BLOCK_SEND_SIZE    = 1 << 9
)

const (
	REQUEST_SEND_BLOCK = iota
	REQUEST_GET_BLOCK
)

const (
	BLOCK_FINISHED = iota
	BLOCK_NOT_FINISHED
)

var (
	RESPONSE_PELEASE_SEND = []byte("ack")

	ErrShutdown = errors.New("Operation Error")
)

type BlockRequest struct {
	TransID     string // It is a UUID
	RequestType byte   // It is an int
}

type BlockStruct struct {
	Finished bool
	Data     []byte
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
			log.Printf("Error accept: %s\n", err.Error())
			continue
		}
		go self.handleRequest(conn)
	}
	return ErrShutdown
}

// DataServer will receive an transaction request and it will call this
// method to add an entry for the transaction
func (self *BlockServer) StartTransaction(leaseID string, c chan []byte) {
	self.transactions[leaseID] = c
}

// Stop one transaction, it may because the transaction terminated or
// the lease is out of date
func (self *BlockServer) StopTransaction(leaseID string) {
	// recover all the errors it will encounter
	defer DummyRecover()

	// if the lease is in the transation map
	if c, ok := self.transactions[leaseID]; ok {
		// close the channel
		close(c)
		// delete the transaction from map
		delete(self.transactions, leaseID)
	}
}

func (self *BlockServer) handleRequest(conn net.Conn) {
	requestBuf := make([]byte, BLOCK_REQUEST_SIZE)
	n, err := conn.Read(requestBuf)
	if err != nil {
		// Log the error and return
		log.Printf("Error read: %s\n", err.Error())
		return
	}

	var req BlockRequest
	err = FromBytes(requestBuf[:n], &req)
	if err != nil {
		// Log the error and return
		log.Println(err.Error())
		return
	}
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
	// anyway, stop transaction
	defer self.StopTransaction(transID)
	// anyway, close the connection
	defer conn.Close()

	buf := make([]byte, BLOCK_BUFFER_SIZE)
	for {
		// ack
		_, err := conn.Write(RESPONSE_PELEASE_SEND)
		if err != nil {
			// Log the error and return
			log.Printf("Error write: %s\n", err.Error())
			self.transactions[transID] <- nil
			return
		}

		n, err := conn.Read(buf)
		if err != nil {
			// Log the error
			log.Printf("Error read: %s\n", err.Error())
			// tell the channel there is error
			// send nil to channel
			self.transactions[transID] <- nil
			return
		}
		var bs BlockStruct
		err = FromBytes(buf[:n], &bs)
		if err != nil {
			// Log the error the return
			log.Println(err.Error())
			self.transactions[transID] <- nil
			return
		}
		if bs.Finished {
			break
		}
		// TODO Question potential racing condition
		// Make the chan limited to 1
		self.transactions[transID] <- bs.Data
	}
}

func (self *BlockServer) handleGetRequest(conn net.Conn, transID string) {
	// anyway, stop transaction
	defer self.StopTransaction(transID)
	reqBuf := make([]byte, BLOCK_REQUEST_SIZE)
	for {
		data, ok := <-self.transactions[transID]
		if !ok {
			// finished
			buf := ToBytes(&BlockStruct{
				Finished: true,
			})
			conn.Write(buf)
			return
		}
		// get the ack from client
		_, err := conn.Read(reqBuf)
		if err != nil {
			log.Printf("Error read: %s\n", err.Error())
			return
		}
		buf := ToBytes(&BlockStruct{
			Finished: false,
			Data:     data,
		})
		// write to client
		_, err = conn.Write(buf)
		if err != nil {
			// if there is an error, close channel
			log.Println(err.Error())
			return
		}
	}
}

func NewBlockServer(conf *config.BlockServerConfig) *BlockServer {
	return &BlockServer{
		conf:         conf,
		blockSize:    conf.BlockSize,
		transactions: make(map[string]chan []byte),
	}
}

type BlockClient struct {
	blockSize int64
	addr      string
}

func NewBlockClient(host string, conf *config.BlockServerConfig) *BlockClient {
	return &BlockClient{
		blockSize: conf.BlockSize,
		addr:      host + ":" + conf.Port,
	}
}

func (self *BlockClient) SendBlock(c chan []byte, transID string) {
	conn, err := net.Dial("tcp", self.addr)
	if err != nil {
		// if there is an error, close channel
		log.Println(err.Error())
		close(c)
		return
	}

	requestBytes := ToBytes(&BlockRequest{
		TransID:     transID,
		RequestType: REQUEST_SEND_BLOCK,
	})

	_, err = conn.Write(requestBytes)
	if err != nil {
		// if there is an error, close channel
		log.Println(err.Error())
		close(c)
		return
	}
	buf := make([]byte, BLOCK_REQUEST_SIZE)
	for {
		// get the ack from server
		_, err := conn.Read(buf)
		if err != nil {
			log.Printf("Error read: %s\n", err.Error())
			close(c)
			return
		}
		b, ok := <-c
		if !ok {
			// sender closed the channel
			// it is done
			buf := ToBytes(&BlockStruct{
				Finished: true,
			})
			_, err = conn.Write(buf)
			break
		}
		// write another
		buf := ToBytes(&BlockStruct{
			Finished: false,
			Data:     b,
		})
		_, err = conn.Write(buf)
		if err != nil {
			// if there is an error, close channel
			log.Println(err.Error())
			close(c)
			return
		}
	}
	return
}

func (self *BlockClient) GetBlock(c chan []byte, transID string) {
	// any way, close the chanel
	defer close(c)
	conn, err := net.Dial("tcp", self.addr)
	// any way, close the conn
	defer conn.Close()
	if err != nil {
		log.Println(err.Error())
		return
	}

	requestBytes := ToBytes(&BlockRequest{
		TransID:     transID,
		RequestType: REQUEST_GET_BLOCK,
	})
	_, err = conn.Write(requestBytes)

	buf := make([]byte, BLOCK_BUFFER_SIZE)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			// Log the error
			log.Printf("Error read: %s\n", err.Error())
			// send nil to channel
			c <- nil
			return
		}
		var bs BlockStruct
		err = FromBytes(buf[:n], &bs)
		if err != nil {
			// Log the error the return
			log.Println(err.Error())
			c <- nil
			return
		}
		if bs.Finished {
			break
		}
		// TODO Question potential racing condition
		// Make the chan limited to 1
		c <- bs.Data
		// ack
		_, err = conn.Write(RESPONSE_PELEASE_SEND)
		if err != nil {
			// Log the error and return
			log.Printf("Error write: %s\n", err.Error())
			c <- nil
			return
		}
	}
}
