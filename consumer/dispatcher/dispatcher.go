package dispatcher

import (
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	log "github.com/sirupsen/logrus"
)

// T reads consume requests submitted to the `Requests()` channel and
// dispatches them to downstream dispatch tiers based on the dispatch key of
// requests. Dispatcher uses `dispatchTierFactory` to resolve a dispatch key
// from a consume request and create `dispatchTier` instances on demand. When a
// down stream dispatch tier is created it is cached in case more requests
// resolving to it will come in the nearest future.
type T struct {
	actorID           *actor.ID
	cfg               *config.Proxy
	factory           Factory
	requestsCh        chan Request
	children          map[string]*expiringTier
	expiredChildrenCh chan Tier
	stoppedChildrenCh chan Tier
	wg                sync.WaitGroup
}

type Request struct {
	Timestamp  time.Time
	Group      string
	Topic      string
	ResponseCh chan<- Response
}

type Response struct {
	Msg consumer.Message
	Err error
}

// Factory defines an interface to create Tiers.
type Factory interface {
	// dispatchKey returns a key that a child created for the specified `req`
	// should have.
	KeyOf(req Request) string

	// NewTier creates a new dispatch tier to handle requests with the
	// specified dispatch key.
	NewTier(key string) Tier
}

// Tier defines a consume request handling tier interface.
type Tier interface {
	// Key returns the dispatch key of the tier.
	Key() string

	// Requests returns a channel to send requests dispatched to the tier to.
	Requests() chan<- Request

	// Start spins up the tier's goroutine(s).
	Start(stoppedCh chan<- Tier)

	// Stop makes all tier goroutines stop and releases all resources.
	Stop()
}

// expiringDispatchTier represents a dispatch tier that expires if not used.
// If a request comes when the tier has already expired but has not yet stopped,
// then a successor instance is created and new requests are queued to it.
// as soon as the original tier is stopped the successor is started to take its
// place.
type expiringTier struct {
	d         *T
	factory   Factory
	instance  Tier
	successor Tier
	timer     *time.Timer
	expired   bool
}

func New(namespace *actor.ID, factory Factory, cfg *config.Proxy) *T {
	d := &T{
		actorID:           namespace.NewChild("dispatcher"),
		cfg:               cfg,
		factory:           factory,
		requestsCh:        make(chan Request, cfg.Consumer.ChannelBufferSize),
		children:          make(map[string]*expiringTier),
		expiredChildrenCh: make(chan Tier, cfg.Consumer.ChannelBufferSize),
		stoppedChildrenCh: make(chan Tier, cfg.Consumer.ChannelBufferSize),
	}
	return d
}

func (d *T) Start() {
	actor.Spawn(d.actorID, &d.wg, d.run)
}

func (d *T) Stop() {
	close(d.requestsCh)
	d.wg.Wait()
}

func (d *T) Requests() chan<- Request {
	return d.requestsCh
}

// run receives consume requests from the `Requests()` channel and dispatches
// them to downstream tiers based on request dispatch key.
func (d *T) run() {
	for {
		select {
		case req, ok := <-d.requestsCh:
			if !ok {
				goto done
			}
			dt := d.resolveTier(req)
			// If the requests buffer is full then either the callers are
			// pulling too aggressively or the Kafka is experiencing issues.
			// Either way we reject requests right away and callers are
			// expected to back off for awhile and repeat their request later.
			select {
			case dt.Requests() <- req:
			default:
				req.ResponseCh <- Response{Err: consumer.ErrTooManyRequests}
			}

		case dt := <-d.expiredChildrenCh:
			d.handleExpired(dt)

		case dt := <-d.stoppedChildrenCh:
			d.handleStopped(dt)
		}
	}
done:
	for _, et := range d.children {
		if !et.expired {
			go et.instance.Stop()
		}
	}
	// The children dispatch tiers will stop as soon as they process all
	// pending requests which may take up to twice the
	// `Config.Consumer.LongPollingTimeout`. The second timeout comes from the
	// successor instance that is started in `handleStopped` to drain its queue.
	for len(d.children) > 0 {
		dt := <-d.stoppedChildrenCh
		if successor := d.handleStopped(dt); successor != nil {
			go successor.Stop()
		}
	}
}

func (d *T) newExpiringTier(parent Factory, key string) *expiringTier {
	dt := parent.NewTier(key)
	dt.Start(d.stoppedChildrenCh)
	timeout := d.cfg.Consumer.RegistrationTimeout
	et := &expiringTier{
		d:        d,
		factory:  parent,
		instance: dt,
		timer:    time.AfterFunc(timeout, func() { d.expiredChildrenCh <- dt }),
	}
	return et
}

// resolveTier returns a downstream dispatch tier corresponding to the dispatch
// key of the specified request. If there is no such tier instance then it is
// created using the associated factory. If the tier exists but stopping at the
// moment due to inactivity timeout, then a successor tier instance is created
// and returned.
func (d *T) resolveTier(req Request) Tier {
	childKey := d.factory.KeyOf(req)
	et := d.children[childKey]
	if et == nil {
		et = d.newExpiringTier(d.factory, childKey)
		d.children[childKey] = et
	}
	if !et.expired && et.timer.Reset(et.d.cfg.Consumer.RegistrationTimeout) {
		return et.instance
	}
	if et.successor == nil {
		et.successor = et.factory.NewTier(et.instance.Key())
	}
	return et.successor
}

// handleExpired marks the respective dispatch tier as expired and triggers its
// asynchronous stop. When the tier is stopped it will notify about that via the
// `stoppedChildrenCh` channel.
func (d *T) handleExpired(dt Tier) {
	log.Infof("<%s> child expired: %s", d.actorID, dt)
	et := d.children[dt.Key()]
	if et == nil || et.instance != dt || et.expired {
		return
	}
	et.expired = true
	go et.instance.Stop()
}

// handleStopped if the specified dispatch tier has a successor then it is
// started and takes over the tier's spot among the downstream dispatch tiers,
// otherwise the tier is deleted.
func (d *T) handleStopped(dt Tier) Tier {
	log.Infof("<%s> child stopped: %s", d.actorID, dt)
	et := d.children[dt.Key()]
	if et == nil {
		return nil
	}
	successor := et.successor
	if successor == nil {
		delete(d.children, dt.Key())
		return nil
	}
	log.Infof("<%s> starting successor: %s", d.actorID, successor)
	et.expired = false
	et.instance = successor
	et.successor = nil
	successor.Start(et.d.stoppedChildrenCh)
	timeout := et.d.cfg.Consumer.RegistrationTimeout
	et.timer = time.AfterFunc(timeout, func() { et.d.expiredChildrenCh <- successor })
	return et.instance
}
