package fs

import (
	"github.com/proj-223/CatFs/client"
	"github.com/proj-223/CatFs/data"
	"github.com/proj-223/CatFs/master"
	proc "github.com/proj-223/CatFs/protocols"
)

func NewClient() *client.CatClient {
	return client.NewCatClient()
}

func ServeMaster(path string) error {
	return master.Serve()
}

func NewDataServer(location proc.ServerLocation) *data.DataServer {
	return data.NewDataServer(location)
}
