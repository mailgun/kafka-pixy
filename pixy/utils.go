package pixy

import (
	"sync"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
)

func goGo(name string, wg *sync.WaitGroup, f func()) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		defer logSc(opeOf(name))
		if wg != nil {
			defer wg.Done()
		}
		f()
	}()
}

// logSc is supposed to be used along with `opeOf` to log boundaries of a
// function, as follows `defer logSc(opeOf(<function-name>))`.
func logSc(scopeName string) {
	log.Infof("<%s> goroutine stopped", scopeName)
}

// opeOf is supposed to be used along with `logSc` to log boundaries of a
// function, as follows `defer logSc(opeOf(<function-name>))`.
func opeOf(scopeName string) string {
	log.Infof("<%s> goroutine started", scopeName)
	return scopeName
}

// toEncoderPreservingNil converts a slice of bytes to `sarama.Encoder` but
// returns `nil` if the passed slice is `nil`.
func toEncoderPreservingNil(b []byte) sarama.Encoder {
	if b != nil {
		return sarama.ByteEncoder(b)
	}
	return nil
}
