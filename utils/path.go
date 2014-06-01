package utils

import (
	"path"
)

func Abs(cur, rpath string) string {
	if path.IsAbs(rpath) {
		return rpath
	}
	return path.Join(cur, rpath)
}
