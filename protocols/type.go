package protocols

import (
	"time"
)

// The Information of a block
type CatBlock struct {
	// The Id of a block
	ID string
	// The location of data server (Indexes)
	// The first one is the primary
	Location []int
}

type CatFileStatus struct {
	// The name of the file
	Filename string
	// The length of the file
	Length int64
	// Time of last status change
	CTime time.Time
	// Time of last modification
	MTime time.Time
	// Time of last access
	ATime time.Time
	// Owner of the file
	Owner string
	// Group of the file
	Group string
	// Permission of the file
	// u:rwx g:rwx o:rwx
	Mode int16
}

const (
	// Read lock
	LEASE_READ = iota
	// Write lock
	LEASE_WRITE
)

type CatLease struct {
	// The id the the ease
	// It works like a transaction ID
	ID string
	// Expire time of the lease
	Expire time.Time
}

type CatFileLease struct {
	// The id the the ease
	// It works like a transaction ID
	ID string
	// Type of the lease
	Type int
	// Expire time of the lease
	Expire time.Time
}
