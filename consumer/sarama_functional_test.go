package consumer

import (
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

const (
	VagrantToxiproxy      = "http://192.168.100.67:8474"
	VagrantKafkaPeers     = "192.168.100.67:9091,192.168.100.67:9092,192.168.100.67:9093,192.168.100.67:9094,192.168.100.67:9095"
	VagrantZookeeperPeers = "192.168.100.67:2181,192.168.100.67:2182,192.168.100.67:2183,192.168.100.67:2184,192.168.100.67:2185"
)

var (
	kafkaAvailable, kafkaRequired bool
	kafkaBrokers                  []string

	ZKProxies    = []string{"zk1", "zk2", "zk3", "zk4", "zk5"}
	KafkaProxies = []string{"kafka1", "kafka2", "kafka3", "kafka4", "kafka5"}
)

func init() {
	if os.Getenv("DEBUG") == "true" {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	seed := time.Now().UTC().UnixNano()
	if tmp := os.Getenv("TEST_SEED"); tmp != "" {
		seed, _ = strconv.ParseInt(tmp, 0, 64)
	}
	sarama.Logger.Println("Using random seed:", seed)
	rand.Seed(seed)

	kafkaPeers := os.Getenv("KAFKA_PEERS")
	if kafkaPeers == "" {
		kafkaPeers = VagrantKafkaPeers
	}
	kafkaBrokers = strings.Split(kafkaPeers, ",")

	if c, err := net.DialTimeout("tcp", kafkaBrokers[0], 5*time.Second); err == nil {
		if err = c.Close(); err == nil {
			kafkaAvailable = true
		}
	}

	kafkaRequired = os.Getenv("CI") != ""
}

func checkKafkaAvailability(t testing.TB) {
	if !kafkaAvailable {
		if kafkaRequired {
			t.Fatalf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		} else {
			t.Skipf("Kafka broker is not available on %s. Set KAFKA_PEERS to connect to Kafka on a different location.", kafkaBrokers[0])
		}
	}
}

func checkKafkaVersion(t testing.TB, requiredVersion string) {
	kafkaVersion := os.Getenv("KAFKA_VERSION")
	if kafkaVersion == "" {
		t.Logf("No KAFKA_VERSION set. This test requires Kafka version %s or higher. Continuing...", requiredVersion)
	} else {
		available := parseKafkaVersion(kafkaVersion)
		required := parseKafkaVersion(requiredVersion)
		if !available.satisfies(required) {
			t.Skipf("Kafka version %s is required for this test; you have %s. Skipping...", requiredVersion, kafkaVersion)
		}
	}
}

func setupFunctionalTest(t testing.TB) {
	checkKafkaAvailability(t)
}

func teardownFunctionalTest(t testing.TB) {
}

type kafkaVersion []int

func (kv kafkaVersion) satisfies(other kafkaVersion) bool {
	var ov int
	for index, v := range kv {
		if len(other) <= index {
			ov = 0
		} else {
			ov = other[index]
		}

		if v < ov {
			return false
		} else if v > ov {
			return true
		}
	}
	return true
}

func parseKafkaVersion(version string) kafkaVersion {
	numbers := strings.Split(version, ".")
	result := make(kafkaVersion, 0, len(numbers))
	for _, number := range numbers {
		nr, _ := strconv.Atoi(number)
		result = append(result, nr)
	}

	return result
}
