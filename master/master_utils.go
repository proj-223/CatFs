package master

import (
	"strings"
)

func PathToElements(path string) []string {
	return strings.Split(path,"/")[1:]
}