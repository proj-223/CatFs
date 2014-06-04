package master

import "sync"

type LockManager struct {
	Lockmap map[string]*sync.Mutex
}

func (self *LockManager) AcquireLock(path string) {
	_, ok := self.Lockmap[path]
	if !ok {
		self.Lockmap[path] = &sync.Mutex{}
	}
	self.Lockmap[path].Lock()
}

func (self *LockManager) ReleaseLock(path string) {
	_, ok := self.Lockmap[path]
	if !ok {
		panic("The file does not exist!")
	}
	self.Lockmap[path].Unlock()
}
