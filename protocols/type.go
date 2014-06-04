package protocols

import (
	"time"
)

type BlockLocation int

// The Information of a block
type CatBlock struct {
	// The Id of a block
	ID string
	// The location of data server (Indexes)
	// The first one is the primary
	Locations []BlockLocation
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

type ServerLocation int

type DataServerStatus struct {
	Location     ServerLocation
	AvaiableSize int64
	DataSize     int64
	TotalSize    int64
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

type MasterCommand struct {
	Command  MasterCommandType
	Machines []ServerLocation
	Blocks   []string
}
