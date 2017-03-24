package actor

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/mailgun/log"
)

type ID struct {
	absoluteName string

	childrenMu       sync.Mutex
	childrenCounters map[string]int32
}

// RootID is the root of the context id hierarchy.
var RootID = &ID{}

// NewChild creates a child id.
func (id *ID) NewChild(nameParts ...interface{}) *ID {
	if len(nameParts) == 0 {
		return id
	}

	var buf bytes.Buffer
	for i, p := range nameParts {
		if i != 0 {
			buf.WriteString("_")
		}
		buf.WriteString(fmt.Sprintf("%v", p))
	}
	name := buf.String()

	id.childrenMu.Lock()
	if id.childrenCounters == nil {
		id.childrenCounters = make(map[string]int32)
	}
	idx := id.childrenCounters[name]
	id.childrenCounters[name] = idx + 1
	id.childrenMu.Unlock()
	return &ID{absoluteName: fmt.Sprintf("%s/%s[%d]", id.absoluteName, name, idx)}
}

func (id *ID) String() string {
	return id.absoluteName
}

// Spawn starts function `f` as a goroutine making it a member of the `wg`
// wait group.
func Spawn(actorID *ID, wg *sync.WaitGroup, f func()) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		log.Infof("<%s> started", actorID)
		defer func() {
			if p := recover(); p != nil {
				log.Errorf("<%s> paniced: %v, stack=%s", actorID, p, debug.Stack())
				panic(p)
			}
			log.Infof("<%s> stopped", actorID)
		}()
		f()
	}()
}
