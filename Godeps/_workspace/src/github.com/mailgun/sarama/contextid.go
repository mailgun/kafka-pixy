package sarama

import (
	"fmt"
	"sync/atomic"
)

type ContextID struct {
	name      string
	prevIndex int32
}

var RootCID = &ContextID{prevIndex: -1}

func (cid *ContextID) NewChild(prefix string) *ContextID {
	idx := atomic.AddInt32(&cid.prevIndex, 1)
	return &ContextID{fmt.Sprintf("%s/%s[%d]", cid.name, prefix, idx), -1}
}

func (cid *ContextID) String() string {
	return cid.name
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
