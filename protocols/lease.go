package protocols

import (
	"code.google.com/p/go-uuid/uuid"
	"time"
)

const (
	// Read lock
	LEASE_READ = iota
	// Write lock
	LEASE_WRITE
)

const (
	LEASE_DURATION = time.Minute
)

type CatLease struct {
	// The id the the ease
	// It works like a transaction ID
	ID string
	// Expire time of the lease
	Expire time.Time
}

type CatFileLease struct {
	// The id the the lease
	// It works like a transaction ID
	ID string
	// Type of the lease
	Type int
	// Expire time of the lease
	Expire time.Time
}

func NewCatLease() *CatLease {
	return &CatLease{
		Expire: time.Now().Add(LEASE_DURATION),
		ID:     uuid.New(),
	}
}

func (self *CatLease) Renew() {
	self.Expire.Add(LEASE_DURATION)
}
