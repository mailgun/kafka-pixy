package actor

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"sync"

	log "github.com/sirupsen/logrus"
)

const fieldActorName = "tid"

// Descriptor
type Descriptor struct {
	absoluteName string
	log          *log.Entry

	childrenMu     sync.Mutex
	childrenCounts map[string]int32
}

var root = Descriptor{log: log.NewEntry(log.StandardLogger())}

// Root returns the root actor descriptor that all other descriptors are either
// direct or indirect descendants of.
func Root() *Descriptor {
	return &root
}

// NewChild creates a child descriptor.
func (d *Descriptor) NewChild(nameParts ...interface{}) *Descriptor {
	if len(nameParts) == 0 {
		return d
	}

	var buf bytes.Buffer
	for i, p := range nameParts {
		if i != 0 {
			buf.WriteString("_")
		}
		buf.WriteString(fmt.Sprintf("%v", p))
	}
	name := buf.String()

	d.childrenMu.Lock()
	if d.childrenCounts == nil {
		d.childrenCounts = make(map[string]int32)
	}
	idx := d.childrenCounts[name]
	d.childrenCounts[name] = idx + 1
	d.childrenMu.Unlock()
	childAbsName := fmt.Sprintf("%s/%s.%d", d.absoluteName, name, idx)
	childLog := d.log.WithField(fieldActorName, childAbsName)
	child := Descriptor{
		absoluteName: childAbsName,
		log:          childLog,
	}
	return &child
}

// AddLogField adds a field to the associated structured log entry. Note that
// this method is not synchronized therefor all AddLogCalls must be performed
// from the same goroutine that created the descriptor and before the
// descriptor is passed to child goroutines.
func (d *Descriptor) AddLogField(key string, value interface{}) {
	d.log = d.log.WithField(key, value)
}

// Log returns logger with associated fields. Fields are inherited from the
// parent logger.
func (d *Descriptor) Log() *log.Entry {
	return d.log
}

func (d *Descriptor) String() string {
	return d.absoluteName
}

// Spawn starts function `f` as a goroutine making it a member of the `wg`
// wait group.
func Spawn(actDesc *Descriptor, wg *sync.WaitGroup, f func()) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		actDesc.Log().Info("started")
		defer func() {
			if p := recover(); p != nil {
				actDesc.Log().Errorf("paniced: %v, stack=%s", p, debug.Stack())
				panic(p)
			}
			actDesc.Log().Info("stopped")
		}()
		f()
	}()
}
