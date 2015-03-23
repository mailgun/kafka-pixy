package pixy

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"

	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type HTTPAPISuite struct {
	addr       string
	httpClient *http.Client
	as         *HTTPAPIServer
	errorCh    <-chan error
	produced   []*production
}

var _ = Suite(&HTTPAPISuite{})

func (s *HTTPAPISuite) SetUpSuite(c *C) {
	InitTestLog()
}

func (s *HTTPAPISuite) SetUpTest(c *C) {
	s.addr = path.Join(os.TempDir(), testSocket)
	os.Remove(s.addr)

	s.httpClient = NewUDSHTTPClient(s.addr)
	var err error
	s.as, err = SpawnHTTPAPIServer(NetworkUnix, s.addr, s)
	if err != nil {
		panic(err)
	}
	s.produced = nil
}

func (s *HTTPAPISuite) TearDownTest(c *C) {
	if s.as != nil {
		s.as.Stop()
		<-s.as.ErrorCh()
	}
}

func (s *HTTPAPISuite) TestInvalidServerAddress(c *C) {
	// When
	as, err := SpawnHTTPAPIServer(NetworkUnix, "/tmp", s)
	// Then
	c.Assert(as, IsNil)
	c.Assert(err.Error(), Equals,
		"failed to create listener, cause=(listen unix /tmp: bind: address already in use)")
}

func (s *HTTPAPISuite) TestPostMessage(c *C) {
	// When
	r, err := s.httpClient.Post("http://_/topics/httpapi-test?key=test-key",
		"", strings.NewReader("foo bar"))
	// Then
	c.Assert(err, IsNil)
	c.Assert(ResponseBody(r), DeepEquals, "")
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	c.Assert(s.produced[0], DeepEquals, &production{
		topic:   "httpapi-test",
		key:     []byte("test-key"),
		message: []byte("foo bar"),
	})
}

func (s *HTTPAPISuite) TestPostMessageEmptyKey(c *C) {
	// When
	r, _ := s.httpClient.Post("http://_/topics/httpapi-test?key=",
		"", strings.NewReader("foo bar"))
	// Then
	c.Assert(ResponseBody(r), DeepEquals, "")
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	c.Assert(s.produced[0], DeepEquals, &production{
		topic:   "httpapi-test",
		key:     []byte{}, // Key is empty
		message: []byte("foo bar"),
	})
}

func (s *HTTPAPISuite) TestPostMessageNoKey(c *C) {
	// When
	r, _ := s.httpClient.Post("http://_/topics/httpapi-test",
		"", strings.NewReader("foo bar"))
	// Then
	c.Assert(ResponseBody(r), DeepEquals, "")
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	c.Assert(s.produced[0], DeepEquals, &production{
		topic:   "httpapi-test",
		key:     nil, // Key is not specified
		message: []byte("foo bar"),
	})
}

func (s *HTTPAPISuite) TestPostMessageNoTopic(c *C) {
	// When
	r, _ := s.httpClient.Post("http://_/topics/", "", strings.NewReader("foo bar"))
	// Then
	c.Assert(r.StatusCode, Equals, http.StatusNotFound)
}

func (s *HTTPAPISuite) TestPostMessageEscapedKey(c *C) {
	// Given
	v := url.Values{}
	v.Add("key", "&/Ой?:")
	u := fmt.Sprintf("http://_/topics/httpapi-test?%s", v.Encode())
	// When
	r, _ := s.httpClient.Post(u, "", strings.NewReader("foo bar"))
	// Then
	c.Assert(ResponseBody(r), DeepEquals, "")
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	c.Assert(s.produced[0], DeepEquals, &production{
		topic:   "httpapi-test",
		key:     []byte("&/Ой?:"),
		message: []byte("foo bar"),
	})
}

func (s *HTTPAPISuite) TestTCPServer(c *C) {
	// Given
	as, err := SpawnHTTPAPIServer(NetworkTCP, "localhost:55500", s)
	c.Assert(err, IsNil)
	// When
	r, err := http.Post("http://localhost:55500/topics/httpapi-test", "",
		strings.NewReader("foo bar"))
	// Then
	c.Assert(err, IsNil)
	c.Assert(ResponseBody(r), DeepEquals, "")
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	c.Assert(s.produced[0], DeepEquals, &production{
		topic:   "httpapi-test",
		key:     nil,
		message: []byte("foo bar"),
	})
	as.Stop()
	<-as.ErrorCh()
}

// Make sure that the server does not respond to requests send via kept alive
// connections after stop.
func (s *HTTPAPISuite) TestRequestAfterStop(c *C) {
	// Given
	_, err := s.httpClient.Post("http://_/topics/httpapi-test?key=1",
		"", strings.NewReader("foo"))
	c.Assert(err, IsNil)
	// When
	s.as.Stop()
	<-s.as.ErrorCh()
	s.as = nil
	// Then
	r, err := s.httpClient.Post("http://_/topics/httpapi-test?key=2",
		"", strings.NewReader("bar"))
	c.Assert(r, IsNil)
	c.Assert(err.Error(), Equals, "Post http://_/topics/httpapi-test?key=2: EOF")
}

func (s *HTTPAPISuite) Produce(topic string, key, message []byte) {
	s.produced = append(s.produced, &production{topic, key, message})
}

type production struct {
	topic   string
	key     []byte
	message []byte
}
