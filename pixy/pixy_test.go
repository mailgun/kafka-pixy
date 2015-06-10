package pixy

import (
	"os"
	"strings"
	"testing"

	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

var (
	testBrokers = []string{"localhost:9092"}
)

func init() {
	if testBrokersEnv := os.Getenv("KAFKA_PEERS"); testBrokersEnv != "" {
		testBrokers = strings.Split(testBrokersEnv, ",")
	}
}

func Test(t *testing.T) {
	TestingT(t)
}
