package protocols

/* Client Data Params */

type PrepareBlockParam struct {
	// The block to send
	Block *CatBlock
}

func (self *PrepareBlockParam) BlockLocation() BlockLocation {
	return self.Block.Locations[0]
}

func (self *PrepareBlockParam) NextPipeParam() *PrepareBlockParam {
	if len(self.Block.Locations) <= 1 {
		// if there is no more blocks
		return nil
	}
	param := &PrepareBlockParam{
		Block: &CatBlock{
			ID:        self.Block.ID,
			Locations: self.Block.Locations[1:],
		},
	}
	return param
}

type SendingBlockParam struct {
	// Transaction id returned by prepare block
	Lease *CatLease
	Block *CatBlock
}

type GetBlockParam struct {
	// Transaction id returned by prepare block
	Block *CatBlock
}

/* Client Master Params */

// Query for blocks of a specific file
type BlockQueryParam struct {
	// The abs path of a file
	Path string
	// The offset of a file
	Offset int64
	// The length wanted
	Length int64
	// The lease of a file
	Lease *CatFileLease
}

type CreateFileParam struct {
	// The abs path of the file
	Path string
	// Ower of the file
	Ower *string
}

const (
	OPEN_MODE_READ = iota
	OPEN_MODE_WRITE
)

type GetBlocksLocationParam struct {
	blocks []*CatBlock
}

type OpenFileParam struct {
	// The abs path of the file
	Path string
	// Ower of the file
	Ower *string
	// Open mode read or write
	Mode int
}

type OpenFileResponse struct {
	// the status of the file
	Filestatus *CatFileStatus
	// the file lease
	Lease *CatFileLease
}

type AddBlockParam struct {
	// The abs path of the file
	Path string
	// The lease of a file
	Lease *CatFileLease
}

type AbandonBlockParam struct {
	// The abs path of the file
	Path string
	// Block
	Block *CatBlock
	// The lease of a file
	Lease *CatFileLease
}

type CloseParam struct {
	// The abs path of the file
	Path string
	// The lease of a file
	Lease *CatFileLease
	// User
	User string
}

type RenameParam struct {
	// Source Path
	Src string
	// Destination Path
	Des string
	// User
	User string
}

type DeleteParam struct {
	// Path of a file or dir
	Path string
	// User
	User string
}

type MkdirParam struct {
	// Path of the dir
	Path string
	// Ower of the dir
	Owner string
}

type ListDirParam struct {
	// Path of the dir
	Path string
	// User
	User string
}

type ListdirResponse struct {
	Files []*CatFileStatus
}

/* DataMaster Param */

type RegisterDataParam struct {
}

type HeartbeatParam struct {
}

type HeartbeatResponse struct {
}

type BlockReportParam struct {
}

type BlockReportResponse struct {
}
