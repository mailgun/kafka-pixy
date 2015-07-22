package pixy

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
)

type none struct{}

var nothing = none{}

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

func getIP() (net.IP, error) {
	interfaceAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	var ipv6 net.IP
	for _, interfaceAddr := range interfaceAddrs {
		if ipAddr, ok := interfaceAddr.(*net.IPNet); ok && !ipAddr.IP.IsLoopback() {
			ipv4 := ipAddr.IP.To4()
			if ipv4 != nil {
				return ipv4, nil
			}
			ipv6 = ipAddr.IP
		}
	}
	if ipv6 != nil {
		return ipv6, nil
	}
	return nil, errors.New("Unknown IP address")
}

// retry keeps calling the `f` function until it succeeds. `shouldRetry` is
// used to check the error code returned by `f` to decide whether it should be
// retried. If `shouldRetry` is ot specified then any non `nil` error will
// result in retry.
func retry(f func() error, shouldRetry func(err error) bool, errorMsg string,
	delay time.Duration, cancelCh <-chan none) (canceled bool) {

	err := f()
	if shouldRetry == nil {
		shouldRetry = func(err error) bool { return err != nil }
	}
	for shouldRetry(err) {
		log.Errorf("%s: err=%v, retryAfter=%v", errorMsg, err, delay)
		select {
		case <-time.After(delay):
		case <-cancelCh:
			return true
		}
		err = f()
	}
	return false
}
