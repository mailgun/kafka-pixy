package args

import (
	"path"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

const MAX_BACKOFF_WAIT = 2 * time.Second

// A ChangeEvent is a representation of an key=value update, delete or expire. Args attempts to match
// a rule to the change and includes the matched rule in the ChangeEvent. If args is unable to match
// a with this change, then ChangeEvent.Rule will be nil
type ChangeEvent struct {
	Group   string
	KeyName string
	Key     string
	Value   []byte
	Deleted bool
	Err     error
	Rule    *Rule
}

func (self *ChangeEvent) SetRule(rule *Rule) {
	self.Group = rule.Group
	self.Rule = rule
}

type Pair struct {
	Key   string
	Value []byte
}

type WatchCancelFunc func()

type Backend interface {
	// Get retrieves a value from a K/V store for the provided key.
	Get(ctx context.Context, key string) (Pair, error)

	// List retrieves all keys and values under a provided key.
	List(ctx context.Context, key string) ([]Pair, error)

	// Set the provided key to value.
	Set(ctx context.Context, key string, value []byte) error

	// Watch monitors store for changes to key.
	Watch(ctx context.Context, key string) <-chan *ChangeEvent

	// Return the root key used to store all other keys in the backend
	GetRootKey() string

	// Closes the connection to the backend and cancels all watches
	Close()
}

func (self *ArgParser) FromBackend(backend Backend) (*Options, error) {

	options, err := self.ParseBackend(backend)
	if err != nil {
		return options, err
	}
	// Apply the etcd values to the commandline and environment variables
	return self.Apply(options)
}

func (self *ArgParser) ParseBackend(backend Backend) (*Options, error) {
	values := self.NewOptions()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer func() { cancel() }()
	//
	for _, rule := range self.rules {
		key := rule.BackendKey(backend.GetRootKey())
		if rule.HasFlag(IsConfigGroup) {
			pairs, err := backend.List(ctx, key)
			if err != nil {
				self.info("args.FromStore().List() fetching '%s' - '%s'", key, err.Error())
				continue
			}
			// Iterate through all the key=values for this group
			for _, pair := range pairs {
				values.Group(rule.Group).Set(path.Base(string(pair.Key)), string(pair.Value))
			}
			continue
		}
		pair, err := backend.Get(ctx, key)
		if err != nil {
			self.info("args.ParseBackend(): Failed to fetch key '%s' - %s", key, err.Error())
			continue
		}
		values.Group(rule.Group).Set(rule.Name, string(pair.Value))
	}
	return values, nil
}

func (self *ArgParser) matchBackendRule(key string, backend Backend) *Rule {
	for _, rule := range self.rules {
		comparePath := key
		if rule.HasFlag(IsConfigGroup) {
			comparePath = path.Dir(key)
		}
		if comparePath == rule.BackendKey(backend.GetRootKey()) {
			return rule
		}
	}
	return nil
}

func (self *ArgParser) Watch(backend Backend, callBack func(*ChangeEvent, error)) WatchCancelFunc {
	var isRunning sync.WaitGroup
	var once sync.Once
	done := make(chan struct{})

	isRunning.Add(1)
	go func() {
		var event *ChangeEvent
		var ok bool
		for {
			// Always attempt to watch, until the user tells us to stop
			ctx, cancel := context.WithCancel(context.Background())

			watchChan := backend.Watch(ctx, backend.GetRootKey())
			once.Do(func() { isRunning.Done() }) // Notify we are watching
			for {
				select {
				case event, ok = <-watchChan:
					if !ok {
						goto Retry
					}
					if event.Err != nil {
						callBack(nil, errors.Wrap(event.Err, "ArgParser.Watch()"))
						goto Retry
					}

					rule := self.matchBackendRule(event.Key, backend)
					if rule != nil {
						event.SetRule(rule)
					}
					callBack(event, nil)
				case <-done:
					cancel()
					return
				}
			}
		Retry:
			// Cancel our current context and sleep
			cancel()
			self.Sleep()
		}
	}()

	// Wait until the go-routine is running before we return, this ensures any updates
	// our application might need from the backend picked up by Watch()
	isRunning.Wait()
	return func() { close(done) }
}

func (self *ArgParser) Sleep() {
	self.attempts = self.attempts + 1
	delay := time.Duration(self.attempts) * 2 * time.Millisecond
	if delay > MAX_BACKOFF_WAIT {
		delay = MAX_BACKOFF_WAIT
	}
	self.log.Printf("WatchEtcd Retry in %v ...", delay)
	time.Sleep(delay)
}
