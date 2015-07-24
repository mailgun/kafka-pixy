package pixy

import (
	"sync"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
)

type none struct{}

func goGo(wg *sync.WaitGroup, f func()) {
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		f()
	}()
}

// logSc is supposed to be used along with `opeOf` to log boundaries of a
// function, as follows `defer logSc(opeOf(<function-name>))`.
// opeOf is supposed to be used along with `logSc` to log boundaries of a
// function, as follows `defer logSc(opeOf(<function-name>))`.
func logScope(funcName string) func() {
	log.Infof("<%s> entered", funcName)
	return func() {
		log.Infof("<%s> leaving", funcName)
	}
}

// toEncoderPreservingNil converts a slice of bytes to `sarama.Encoder` but
// returns `nil` if the passed slice is `nil`.
func toEncoderPreservingNil(b []byte) sarama.Encoder {
	if b != nil {
		return sarama.StringEncoder(b)
	}
	return nil
}
