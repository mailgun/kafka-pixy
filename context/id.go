package context

import (
	"fmt"
	"sync"

	"github.com/mailgun/log"
)

type ID struct {
	path     string
	counters map[string]int32
	lock     sync.Mutex
}

// RootID is the root of the context id hierarchy.
var RootID = &ID{}

// NewChild creates a child id.
func (id *ID) NewChild(name string) *ID {
	id.lock.Lock()
	if id.counters == nil {
		id.counters = make(map[string]int32)
	}
	idx := id.counters[name]
	id.counters[name] = idx + 1
	id.lock.Unlock()
	return &ID{path: fmt.Sprintf("%s/%s[%d]", id.path, name, idx)}
}

func (id *ID) String() string {
	return id.path
}

func (id *ID) LogScope(args ...interface{}) func() {
	if len(args) == 0 {
		log.Infof("<%v> entered", id)
	} else {
		log.Infof("<%v> entered: %s", id, fmt.Sprint(args))
	}
	return func() {
		log.Infof("<%v> leaving", id)
	}
}
