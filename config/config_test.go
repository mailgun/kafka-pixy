package config

import (
	"errors"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type ConfigSuite struct{}

var _ = Suite(&ConfigSuite{})

func (s *ConfigSuite) TestGetIP(c *C) {
	ip, err := getIP()
	c.Assert(err, IsNil)
	c.Assert(ip.String(), Matches, "\\d+.\\d+.\\d+.\\d+")
	c.Assert(ip.String(), Not(Equals), "127.0.0.1")
}

func (s *ConfigSuite) TestFromYAMLEmpty(c *C) {
	cfg := Default()
	cfg.ClientID = "ID"
	data := []byte("")

	// When
	err := cfg.FromYAML(data)

	// Then
	c.Assert(err, IsNil)
	expected := Default()
	expected.ClientID = "ID"
	c.Assert(cfg, DeepEquals, expected)
}

// Configuration fields that are not explicitly mentioned if the YAML data are
// left intact.
func (s *ConfigSuite) TestFromYAML(c *C) {
	cfg := Default()
	data := []byte("" +
		"client_id: foo\n" +
		"kafka:\n" +
		"  seed_peers:\n" +
		"    - 192.168.19.2:9092\n" +
		"    - 192.168.19.3:9092\n" +
		"consumer:\n" +
		"  long_polling_timeout: 5s\n")

	// When
	err := cfg.FromYAML(data)

	// Then
	c.Assert(err, IsNil)

	expected := Default()
	expected.ClientID = "foo"
	expected.Kafka.SeedPeers = []string{"192.168.19.2:9092", "192.168.19.3:9092"}
	expected.Consumer.LongPollingTimeout = 5 * time.Second
	c.Assert(cfg, DeepEquals, expected)
}

// If YAML data is invalid then the original config is not changed.
func (s *ConfigSuite) TestFromYAMLInvalid(c *C) {
	cfg := Default()
	cfg.ClientID = "bar"
	data := []byte("" +
		"client_id: foo\n" +
		"kafka:\n" +
		"  seed_peers:\n" +
		"    - 192.168.19.2:9092\n" +
		"    - 192.168.19.3:9092\n" +
		"consumer:\n" +
		"  long_polling_timeout: Kaboom!\n")

	// When
	err := cfg.FromYAML(data)

	// Then
	c.Assert(err, DeepEquals, errors.New("failed to parse config: err=(yaml: unmarshal errors:\n  line 7: cannot unmarshal !!str `Kaboom!` into time.Duration)"))
	expected := Default()
	expected.ClientID = "bar"
	c.Assert(cfg, DeepEquals, expected)
}

// default.yaml contains the same configuration as returned by Default()
func (s *ConfigSuite) TestFromYAMLFile(c *C) {
	cfg := &T{}
	cfg.ClientID = "ID"

	// When
	err := cfg.FromYAMLFile("../default.yaml")

	// Then
	c.Assert(err, IsNil)
	expected := Default()
	expected.ClientID = "ID"
	c.Assert(cfg, DeepEquals, expected)
}
