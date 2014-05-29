package protocols

import (
	"github.com/proj-223/CatFs/utils"
	"time"
)

type BlockLocation int

func (self BlockLocation) DataServer(pool *ClientPool) *DataRPCClient {
	return pool.DataServer(int(self))
}

func (self BlockLocation) BlockClient(pool *ClientPool) *utils.BlockClient {
	return pool.NewBlockClient(int(self))
}

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
