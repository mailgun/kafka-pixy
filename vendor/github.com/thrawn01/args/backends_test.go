package args_test

import (
	"fmt"

	"path"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/thrawn01/args"
	"golang.org/x/net/context"
)

var watchChan chan *args.ChangeEvent

type TestBackend struct {
	keys  map[string]args.Pair
	lists map[string][]args.Pair
	close chan struct{}
}

func NewTestBackend() args.Backend {
	return &TestBackend{
		keys: map[string]args.Pair{
			"/root/bind": args.Pair{Key: "bind", Value: []byte("thrawn01.org:3366")}},
		lists: map[string][]args.Pair{
			"/root/endpoints": []args.Pair{
				args.Pair{Key: "endpoint1", Value: []byte("http://endpoint1.com:3366")},
				args.Pair{Key: "endpoint2", Value: []byte(`{ "host": "endpoint2", "port": "3366" }`)},
			},
			"/root/watch": []args.Pair{
				args.Pair{Key: "endpoint1", Value: []byte("http://endpoint1.com:3366")},
			},
		},
	}
}

func (self *TestBackend) Get(ctx context.Context, key string) (args.Pair, error) {
	pair, ok := self.keys[key]
	if !ok {
		return args.Pair{}, errors.New(fmt.Sprintf("'%s' not found", key))
	}
	return pair, nil
}

func (self *TestBackend) List(ctx context.Context, key string) ([]args.Pair, error) {
	pairs, ok := self.lists[key]
	if !ok {
		return []args.Pair{}, errors.New(fmt.Sprintf("'%s' not found", key))
	}
	return pairs, nil
}

func (self *TestBackend) Set(ctx context.Context, key string, value []byte) error {
	self.keys[key] = args.Pair{Key: key, Value: value}
	return nil
}

// Watch monitors store for changes to key.
func (self *TestBackend) Watch(ctx context.Context, key string) <-chan *args.ChangeEvent {
	changeChan := make(chan *args.ChangeEvent, 2)

	go func() {
		var event *args.ChangeEvent
		select {
		case event = <-watchChan:
			changeChan <- event
		case <-self.close:
			close(changeChan)
			return
		}
	}()
	return changeChan
}

func (self *TestBackend) Close() {
	if self.close != nil {
		close(self.close)
	}
}

func (self *TestBackend) GetRootKey() string {
	return "/root"
}

func NewChangeEvent(key, value string) *args.ChangeEvent {
	return &args.ChangeEvent{
		KeyName: path.Base(key),
		Key:     key,
		Value:   []byte(value),
		Deleted: false,
		Err:     nil,
	}
}

var _ = Describe("backend", func() {
	var log *TestLogger
	var backend args.Backend

	BeforeEach(func() {
		backend = NewTestBackend()
		log = NewTestLogger()
		watchChan = make(chan *args.ChangeEvent, 1)
	})

	AfterEach(func() {
		if backend != nil {
			backend.Close()
		}
	})

	Describe("args.FromBackend()", func() {
		It("Should fetch 'bind' value from backend", func() {
			parser := args.NewParser()
			parser.SetLog(log)
			parser.AddConfig("--bind")

			opts, err := parser.FromBackend(backend)
			Expect(err).To(BeNil())
			Expect(log.GetEntry()).To(Equal(""))
			Expect(opts.String("bind")).To(Equal("thrawn01.org:3366"))
		})
	})

	It("Should use List() when fetching Config Groups", func() {
		parser := args.NewParser()
		parser.SetLog(log)
		parser.AddConfigGroup("endpoints")

		opts, err := parser.FromBackend(backend)
		Expect(err).To(BeNil())
		Expect(log.GetEntry()).To(Equal(""))
		Expect(opts.Group("endpoints").ToMap()).To(Equal(map[string]interface{}{
			"endpoint1": "http://endpoint1.com:3366",
			"endpoint2": `{ "host": "endpoint2", "port": "3366" }`,
		}))
	})

	It("Should return an error if config option not found in the backend", func() {
		parser := args.NewParser()
		parser.SetLog(log)
		parser.AddConfig("--missing")

		opts, err := parser.FromBackend(backend)
		Expect(err).To(BeNil())
		Expect(log.GetEntry()).To(ContainSubstring("not found"))
		Expect(opts.String("missing")).To(Equal(""))
	})

	It("Should call Watch() to watch for new values", func() {
		parser := args.NewParser()
		parser.SetLog(log)
		parser.AddConfigGroup("watch")

		_, err := parser.FromBackend(backend)
		opts := parser.GetOpts()
		Expect(err).To(BeNil())
		Expect(log.GetEntry()).To(Equal(""))
		Expect(opts.Group("watch").ToMap()).To(Equal(map[string]interface{}{
			"endpoint1": "http://endpoint1.com:3366",
		}))

		done := make(chan struct{})

		cancelWatch := parser.Watch(backend, func(event *args.ChangeEvent, err error) {
			// Always check for errors
			if err != nil {
				fmt.Printf("Watch Error - %s\n", err.Error())
				close(done)
				return
			}
			parser.Apply(opts.FromChangeEvent(event))
			// Tell the test to continue, Change event was handled
			close(done)
		})
		// Add a new endpoint
		watchChan <- NewChangeEvent("/root/watch/endpoint2", "http://endpoint2.com:3366")
		// Wait until the change event is handled
		<-done
		// Stop the watch
		cancelWatch()
		// Get the updated options
		opts = parser.GetOpts()

		Expect(log.GetEntry()).To(Equal(""))
		Expect(opts.Group("watch").ToMap()).To(Equal(map[string]interface{}{
			"endpoint1": "http://endpoint1.com:3366",
			"endpoint2": "http://endpoint2.com:3366",
		}))
	})
})
