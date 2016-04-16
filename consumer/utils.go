package consumer

import "sync"

// spawn starts function `f` as a goroutine making it a member of the `wg`
// wait group.
func spawn(wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}
