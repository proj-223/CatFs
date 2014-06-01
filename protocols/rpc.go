package protocols

// client: client
// server: data server
type ClientData interface {

	// Prepare send a block to datanode
	PrepareSendBlock(param *PrepareBlockParam, lease *CatLease) error

	// Wait util blocks reach destination
	// The block will be sent by a pipeline
	SendingBlock(param *SendingBlockParam, succ *bool) error

	// TODO Delete a specific block

	// Get the block from data server
	// Will start an tcp connect to request block
	GetBlock(param *GetBlockParam, lease *CatLease) error
}

// client: client
// server: master server
type ClientMaster interface {

	// Get location of the block of the specified file within the specified range
	GetBlockLocation(query *BlockQueryParam, blocks *GetBlocksLocationResponse) error

	// Create a file in a given path
	Create(param *CreateFileParam, response *OpenFileResponse) error

	// Open a file to add block
	Open(param *OpenFileParam, response *OpenFileResponse) error

	// TODO Append

	// Drop a block
	AbandonBlock(param *AbandonBlockParam, succ *bool) error

	// Add a block to a specific path (file)
	AddBlock(param *AddBlockParam, block *CatBlock) error

	// Complete an operation,
	// delete the lease (lock)
	Close(param *CloseParam, succ *bool) error

	// Rename
	Rename(param *RenameParam, succ *bool) error

	// Delete a file
	Delete(param *DeleteParam, succ *bool) error

	// Create a dir
	Mkdirs(param *MkdirParam, succ *bool) error

	// List dir
	Listdir(param *ListDirParam, response *ListdirResponse) error

	// Renew a lease
	RenewLease(oldLease *CatFileLease, newLease *CatFileLease) error

	// File info
	GetFileInfo(path string, filestatus *CatFileStatus) error
}

// client: data server
// server: master server
type DataMaster interface {

	// Register a data server
	RegisterDataServer(param *RegisterDataParam, succ *bool) error

	// Send heartbeat to master
	SendHeartbeat(param *HeartbeatParam, rep *HeartbeatResponse) error

	// Send blockreport to master
	BlockReport(param *BlockReportParam, rep *BlockReportResponse) error
}

// client: master server
// server: master backup server
type MasterBackup interface {
	// TODO
}

// client: data server
// server: data server
type InterData interface {

	// TODO

}

type MasterProtocol interface {
	ClientMaster
	DataMaster
	MasterBackup
}

type DataProtocol interface {
	ClientData
	InterData
}
