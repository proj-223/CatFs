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

func (self *CatLease) New() {
	self.ID = uuid.New()
	self.Expire = time.Now().Add(LEASE_DURATION)
}

func NewFileLease(tp int) *CatFileLease {
	var lease CatFileLease
	lease.New(tp)
	return &lease
}

func (self *CatFileLease) New(tp int) {
	self.ID = uuid.New()
	self.Type = tp
	self.Expire = time.Now().Add(LEASE_DURATION)
}

func (self *CatFileLease) Renew(oldLease *CatFileLease) {
	self.ID = oldLease.ID
	self.Type = oldLease.Type
	self.Expire = time.Now().Add(LEASE_DURATION)
}

func (self *CatLease) HasInit() bool {
	return len(self.ID) != 0
}
