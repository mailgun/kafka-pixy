package sarama

import (
	"fmt"
	"sync"
)

type ContextID struct {
	path     string
	counters map[string]int32
	lock     sync.Mutex
}

var RootCID = &ContextID{}

func (cid *ContextID) NewChild(name string) *ContextID {
	cid.lock.Lock()
	if cid.counters == nil {
		cid.counters = make(map[string]int32)
	}
	idx := cid.counters[name]
	cid.counters[name] = idx + 1
	cid.lock.Unlock()
	return &ContextID{path: fmt.Sprintf("%s/%s[%d]", cid.path, name, idx)}
}

func (cid *ContextID) String() string {
	return cid.path
}

func (cid *ContextID) LogScope(args ...interface{}) func() {
	if len(args) == 0 {
		Logger.Printf("<%v> entered", cid)
	} else {
		Logger.Printf("<%v> entered: %s", cid, fmt.Sprint(args))
	}
	return func() {
		Logger.Printf("<%v> leaving", cid)
	}
}
