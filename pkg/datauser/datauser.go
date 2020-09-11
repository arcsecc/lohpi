package datauser

import (
	"sync"
)

type DataUser struct {
	name string

	// Interpreted as a single 
	policyAttributes string
	attrLock sync.RWMutex
}

func NewDataUser(name string) *DataUser {
	return &DataUser{
		name:		name, 
		attrLock:	sync.RWMutex{},
	}
}

func (d *DataUser) SetPolicyAttributes(attr string) {
	d.attrLock.Lock()
	defer d.attrLock.Unlock()
	d.policyAttributes = attr
}

func (d *DataUser) GetPolicyAttributes() string {
	d.attrLock.RLock()
	d.attrLock.RUnlock()
	return d.policyAttributes
}