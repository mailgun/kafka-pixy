package pixy

import (
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"sync"
)

func goGo(name string, wg *sync.WaitGroup, f func()) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		defer logExit(logEnter(name))
		if wg != nil {
			defer wg.Done()
		}
		f()
	}()
}

func logExit(name string) {
	log.Infof("<%s> goroutine stopped", name)
}

func logEnter(name string) string {
	log.Infof("<%s> goroutine started", name)
	return name
}
