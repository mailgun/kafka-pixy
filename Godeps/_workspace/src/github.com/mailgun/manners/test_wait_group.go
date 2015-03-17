package manners

import (
	"sync"
)

type waitgroup interface {
	Add(int)
	Done()
	Wait()
}

// testWg is a replica of a sync.WaitGroup that can be introspected.
type testWg struct {
	sync.Mutex
	count        int
	waitCalled   chan int
	countChanged chan int
}

func newTestWg() *testWg {
	return &testWg{
		waitCalled:   make(chan int, 1),
		countChanged: make(chan int, 1024),
	}
}

func (wg *testWg) Add(delta int) {
	wg.Lock()
	wg.count++
	wg.countChanged <- wg.count
	wg.Unlock()
}

func (wg *testWg) Done() {
	wg.Lock()
	wg.count--
	wg.countChanged <- wg.count
	wg.Unlock()
}

func (wg *testWg) Wait() {
	wg.Lock()
	wg.waitCalled <- wg.count
	wg.Unlock()
}
