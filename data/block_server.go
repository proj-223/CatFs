package data

import (
	"github.com/proj-223/CatFs/config"
	proc "github.com/proj-223/CatFs/protocols"
	"github.com/proj-223/CatFs/protocols/pool"
	"io"
	"log"
	"net"
)

const (
	Block_Server_START_MSG = "CatFS Data Block Server are start: %s\n"
)

type Transaction struct {
	ID        string // lease id
	receivers []chan<- []byte
	provider  <-chan []byte
	Done      chan bool
}

func NewReadTransaction(leaseID string, done chan bool, receivers ...chan<- []byte) *Transaction {
	var workReceivers []chan<- []byte
	for _, receiver := range receivers {
		if receiver != nil {
			workReceivers = append(workReceivers, receiver)
		}
	}
	return &Transaction{
		ID:        leaseID,
		receivers: workReceivers,
		Done:      done,
	}
}

func NewProviderTransaction(leaseID string, provider <-chan []byte) *Transaction {
	return &Transaction{
		ID:       leaseID,
		provider: provider,
	}
}

type BlockServer struct {
	transactions map[string]*Transaction
	port         string
	location     proc.ServerLocation
	leaseManager *LeaseManager
}

// Start by DataNode
// It will start an go routine waiting for block request
func (self *BlockServer) Serve() error {
	listener, err := net.Listen("tcp", ":"+self.port)
	if err != nil {
		return err
	}
	log.Printf(Block_Server_START_MSG, self.port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accept: %s\n", err.Error())
			continue
		}
		go self.handleRequest(conn)
	}
	return pool.ErrShutdown
}

// Get transactions
func (self *BlockServer) Transactions() map[string]*Transaction {
	return self.transactions
}

func (self *BlockServer) Transaction(transID string) *Transaction {
	tran, ok := self.transactions[transID]
	if !ok {
		return nil
	}
	return tran
}

// DataServer will receive an transaction request and it will call this
// method to add an entry for the transaction
func (self *BlockServer) StartTransaction(tran *Transaction) {
	self.transactions[tran.ID] = tran
}

// Finish the transaction
func (self *BlockServer) FinishTransaction(leaseID string) {
	// if the lease is in the transation map
	if tran, ok := self.transactions[leaseID]; ok {
		go doneChan(self.transactions[leaseID].Done)
		// close the channel
		for _, c := range tran.receivers {
			go closeByteChan(c)
		}
	}
}

func (self *BlockServer) redirect(leaseID string, b []byte) {
	for _, c := range self.transactions[leaseID].receivers {
		c <- b
	}
}

func (self *BlockServer) handleRequest(conn net.Conn) {
	requestBuf := make([]byte, pool.BLOCK_REQUEST_SIZE)
	n, err := conn.Read(requestBuf)
	if err != nil {
		// Log the error and return
		log.Printf("Error read: %s\n", err.Error())
		return
	}

	var req pool.BlockRequest
	err = pool.FromBytes(requestBuf[:n], &req)
	if err != nil {
		// Log the error and return
		log.Println(err.Error())
		return
	}
	switch int(req.RequestType) {
	case pool.REQUEST_SEND_BLOCK:
		// if the request is to send a block to server
		self.handleSendRequest(conn, req.TransID)
	case pool.REQUEST_GET_BLOCK:
		// if the request is get a block from server
		self.handleGetRequest(conn, req.TransID)
	}
}

func (self *BlockServer) handleSendRequest(conn net.Conn, transID string) {
	// anyway, stop transaction
	defer self.FinishTransaction(transID)
	// anyway, close the connection
	defer conn.Close()
	// make the transaction done
	// ack
	_, err := conn.Write(pool.RESPONSE_PELEASE_SEND)
	if err != nil {
		// Log the error and return
		log.Printf("Error write: %s\n", err.Error())
		self.redirect(transID, nil)
		return
	}
	buf := make([]byte, pool.BLOCK_BUFFER_SIZE_PACED)
	for {
		n, err := conn.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Log the error
			log.Printf("Error read: %s\n", err.Error())
			// tell the channel there is error
			// send nil to channel
			self.redirect(transID, nil)
			return
		}
		self.redirect(transID, buf[:n])
	}
}

func (self *BlockServer) handleGetRequest(conn net.Conn, transID string) {
	// anyway, finish transaction
	defer self.FinishTransaction(transID)
	defer conn.Close()
	for {
		data, ok := <-self.transactions[transID].provider
		if !ok {
			return
		}
		// write to client
		_, err := conn.Write(data)
		if err != nil {
			// if there is an error
			log.Println(err.Error())
			return
		}
	}
}

func (self *BlockServer) registerLeaseListener() {
	self.leaseManager.OnRemoveLease(func(lease *proc.CatLease) {
		// if the lease is in the transation map
		if _, ok := self.transactions[lease.ID]; ok {
			// delete the transaction from map
			delete(self.transactions, lease.ID)
		}
	})
}

func NewBlockServer(location proc.ServerLocation, leaseManager *LeaseManager) *BlockServer {
	port := config.BlockServerPort(int(location))
	bs := &BlockServer{
		transactions: make(map[string]*Transaction),
		leaseManager: leaseManager,
		port:         port,
		location:     location,
	}
	bs.registerLeaseListener()
	return bs
}
