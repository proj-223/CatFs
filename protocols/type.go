package protocols

import (
	"time"
)

type ServerLocation int

// The Information of a block
type CatBlock struct {
	// The Id of a block
	ID string
	// The location of data server (Indexes)
	// The first one is the primary
	Locations []ServerLocation
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
	// Is Dir
	IsDir bool
	// Owner of the file
	Owner string
	// Group of the file
	Group string
	// Permission of the file
	// u:rwx g:rwx o:rwx
	Mode int16
}

type DataServerStatus struct {
	Location     ServerLocation
	AvaiableSize uint64
	DataSize     uint64
	TotalSize    uint64
	Errors       []string
	BlockReports map[string]*DataBlockReport
}

type BlockStatus int

const (
	BLOCK_OK BlockStatus = iota
	BLOCK_Err
)

type DataBlockReport struct {
	ID     string
	Status BlockStatus
}

type MasterCommandType int

const (
	// remove some blocks
	// Blocks is the uuid of the blocks
	// DstMachine is not important
	CleanCommand MasterCommandType = iota
	// Copy blocks from a machine to another machine
	// Blocks is the uuid of the blocks
	// DstMachine is the destination of the blocks
	MigrationCommand
)

type MasterCommand struct {
	Command    MasterCommandType
	Blocks     []string
	DstMachine ServerLocation
}
