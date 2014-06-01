package master

import "sync"

type LockManager struct {
	lockmap map[string]*sync.Mutex
}

func (self *LockManager) AcquireLock(path string) {
	_, ok := self.lockmap[path]
	if !ok {
		self.lockmap[path] = &sync.Mutex{}
	}
	self.lockmap[path].Lock()
}

func (self *LockManager) ReleaseLock(path string) {
	_, ok := self.lockmap[path]
	if !ok {
		panic("The file does not exist!")
	}
	self.lockmap[path].Unlock()
}
