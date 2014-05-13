package protocols

// client: client
// server: data server
type ClientDataServer interface {
}

// client: client
// server: metaserver
type ClientMetaServer interface {
}

// client: dataserver
// server: metaserver
type DataServerMetaServer interface {
}

// client: metaserver
// server: metabackupserver
type MetaServerBackup interface {
}

// client: dataserver
// server: dataserver
type DataServerDataServer interface {
}
