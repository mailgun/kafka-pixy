package test_helpers

import (
	"sync"
)

type WaitGroup struct {
	sync.Mutex
	count        int
	WaitCalled   chan int
	CountChanged chan int
}

func NewWaitGroup() *WaitGroup {
	return &WaitGroup{
		WaitCalled:   make(chan int, 1),
		CountChanged: make(chan int, 1024),
	}
}

func (wg *WaitGroup) Add(delta int) {
	wg.Lock()
	wg.count++
	wg.CountChanged <- wg.count
	wg.Unlock()
}

func (wg *WaitGroup) Done() {
	wg.Lock()
	wg.count--
	wg.CountChanged <- wg.count
	wg.Unlock()
}

func (wg *WaitGroup) Wait() {
	wg.Lock()
	wg.WaitCalled <- wg.count
	wg.Unlock()
}
