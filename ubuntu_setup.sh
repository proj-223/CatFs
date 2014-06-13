#!/bin/bash
sudo apt-get install -y git golang mercurial
mkdir $HOME/go
export GOPATH=$HOME/go
export PATH=$HOME/go/bin:$PATH
go get github.com/proj-223/CatFs
cd $GOPATH/src/github.com/proj-223/CatFs
git checkout -b benchmark origin/benchmark
go install ./...
echo "export GOPATH=$HOME/go" >> $HOME/.bashrc
echo "export PATH=$HOME/go/bin:$PATH" >> $HOME/.bashrc
