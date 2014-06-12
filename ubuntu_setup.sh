#!/bin/bash
sudo apt-get install -y git golang mercurial
mkdir ~/go
export GOPATH=~/go
export PATH=~/go/bin:$PATH
go get github.com/proj-223/CatFs
#cd $GOPATH/src/github.com/proj-223/CatFs && git checkout -b benchmark
origin/benchmark && go install ./...
echo "export GOPATH=~/go" >> ~/.bashrc
echo "export PATH=~/go/bin:$PATH" >> ~/.bashrc
