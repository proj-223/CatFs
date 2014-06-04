package master

import (
	"strings"
)

func PathToElements(path string) []string {
	if(path != "/") {
		return strings.Split(path, "/")[1:]
	} else {
		return make([]string, 0)
	}
}
