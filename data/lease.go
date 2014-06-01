package data

import (
	proc "github.com/proj-223/CatFs/protocols"
)

type LeaseManager struct {
	leases              map[string]*proc.CatLease
	addLeaseListener    []func(lease *proc.CatLease)
	removeLeaseListener []func(lease *proc.CatLease)
}

func (self *LeaseManager) AddLease(lease *proc.CatLease) {
	self.leases[lease.ID] = lease
	for _, listener := range self.addLeaseListener {
		listener(lease)
	}
}

func (self *LeaseManager) RemoveLease(lease *proc.CatLease) {
	if _, ok := self.leases[lease.ID]; ok {
		delete(self.leases, lease.ID)
	}
	for _, listener := range self.removeLeaseListener {
		listener(lease)
	}
}

func (self *LeaseManager) OnAddLease(f func(lease *proc.CatLease)) {
	self.addLeaseListener = append(self.addLeaseListener, f)
}

func (self *LeaseManager) OnRemoveLease(f func(lease *proc.CatLease)) {
	self.removeLeaseListener = append(self.removeLeaseListener, f)
}

func NewLeaseManager() *LeaseManager {
	return new(LeaseManager)
}
