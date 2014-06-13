package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

func LoadConfig(path string) error {
	fi, _ := os.Open(path)
	defer fi.Close()
	decoder := json.NewDecoder(fi)
	var _conf *MachineConfig
	err := decoder.Decode(&_conf)
	if err != nil {
		log.Printf("Error Parse Configuration: %s", err.Error())
		return err
	}
	conf = _conf
	return nil
}

func WriteDefautConfig() error {
	encoded, err := json.MarshalIndent(conf, "", "    ")
	if err != nil {
		log.Printf("Error Encode Configuration: %s", err.Error())
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func ReplicaCount() int {
	return conf.ReplicaCount()
}

func BlockSize() int64 {
	return conf.BlockSize()
}

func MasterPort() string {
	return conf.MasterPort()
}

func MasterAddr() string {
	return conf.MasterAddr()
}

func DataServerPort(index int) string {
	return conf.DataServerPort(index)
}

func BlockServerPort(index int) string {
	return conf.BlockServerPort(index)
}

func DataServerAddr(index int) string {
	return conf.DataServerAddr(index)
}

func DataServerHost(index int) string {
	return conf.DataServerHost(index)
}

func DataServerAddrs() []string {
	return conf.DataServerAddrs()
}

func BlockServerAddr(index int) string {
	return conf.BlockServerAddr(index)
}

func BlockPath(index int) string {
	return conf.BlockPath(index)
}

func HeartBeatInterval() time.Duration {
	return conf.HeartBeatInterval()
}
