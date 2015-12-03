package consumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	"github.com/mailgun/kafka-pixy/config"
)

// dispatcher reads consume requests submitted to the `requests()` channel
// and dispatches them to downstream dispatch tiers based on the dispatch key
// of requests. Dispatcher uses `dispatchTierFactory` to resolve a dispatch
// key from a consume request and create `dispatchTier` instances on demand.
// When a down stream dispatch tier is created it is cached in case more
// requests resolving to it will come in the nearest future.
type dispatcher struct {
	contextID         *sarama.ContextID
	cfg               *config.T
	factory           dispatchTierFactory
	requestsCh        chan consumeRequest
	children          map[string]*expiringDispatchTier
	expiredChildrenCh chan dispatchTier
	stoppedChildrenCh chan dispatchTier
	wg                sync.WaitGroup
}

// dispatchTier defines a consume request handling tier interface.
type dispatchTier interface {
	// key returns the dispatch key of the tier.
	key() string
	// requests returns a channel to send requests dispatched to the tier to.
	requests() chan<- consumeRequest
	// start spins up the tier's goroutine(s).
	start(stoppedCh chan<- dispatchTier)
	// stop makes all tier goroutines stop and releases all resources.
	stop()
}

// dispatchTierFactory defines an interface to create dispatchTiers.
type dispatchTierFactory interface {
	// dispatchKey returns a key that a child created for the specified `req`
	// should have.
	dispatchKey(req consumeRequest) string
	// newDispatchTier creates a new dispatch tier to handle requests with the
	// specified dispatch key.
	newDispatchTier(key string) dispatchTier
}

// expiringDispatchTier represents a dispatch tier that expires if not used.
// If a request comes when the tier has already expired but has not yet stopped,
// then a successor instance is created and new requests are queued to it.
// as soon as the original tier is stopped the successor is started to take its
// place.
type expiringDispatchTier struct {
	d         *dispatcher
	factory   dispatchTierFactory
	instance  dispatchTier
	successor dispatchTier
	timer     *time.Timer
	expired   bool
}

func newDispatcher(baseCID *sarama.ContextID, factory dispatchTierFactory, cfg *config.T) *dispatcher {
	d := &dispatcher{
		contextID:         baseCID.NewChild("dispatcher"),
		cfg:               cfg,
		factory:           factory,
		requestsCh:        make(chan consumeRequest, cfg.Consumer.ChannelBufferSize),
		children:          make(map[string]*expiringDispatchTier),
		expiredChildrenCh: make(chan dispatchTier, cfg.Consumer.ChannelBufferSize),
		stoppedChildrenCh: make(chan dispatchTier, cfg.Consumer.ChannelBufferSize),
	}
	return d
}

func (d *dispatcher) start() {
	spawn(&d.wg, d.run)
}

func (d *dispatcher) stop() {
	close(d.requestsCh)
	d.wg.Wait()
}

func (d *dispatcher) requests() chan<- consumeRequest {
	return d.requestsCh
}

// run receives consume requests from the `requests()` channel and dispatches
// them to downstream tiers based on request dispatch key.
func (d *dispatcher) run() {
	defer d.contextID.LogScope()()
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
			case dt.requests() <- req:
			default:
				overflowErr := ErrBufferOverflow(fmt.Errorf("<%s> buffer overflow", dt))
				req.replyCh <- consumeResult{Err: overflowErr}
			}

		case dt := <-d.expiredChildrenCh:
			d.handleExpired(dt)

		case dt := <-d.stoppedChildrenCh:
			d.handleStopped(dt)
		}
	}
done:
	for _, edt := range d.children {
		if !edt.expired {
			go edt.instance.stop()
		}
	}
	// The children dispatch tiers will stop as soon as they process all
	// pending requests which may take up to twice the
	// `Config.Consumer.LongPollingTimeout`. The second timeout comes from the
	// successor instance that is started in `handleStopped` to drain its queue.
	for len(d.children) > 0 {
		dt := <-d.stoppedChildrenCh
		if successor := d.handleStopped(dt); successor != nil {
			go successor.stop()
		}
	}
}

func (d *dispatcher) newExpiringDispatchTier(parent dispatchTierFactory, key string) *expiringDispatchTier {
	dt := parent.newDispatchTier(key)
	dt.start(d.stoppedChildrenCh)
	timeout := d.cfg.Consumer.RegistrationTimeout
	edt := &expiringDispatchTier{
		d:        d,
		factory:  parent,
		instance: dt,
		timer:    time.AfterFunc(timeout, func() { d.expiredChildrenCh <- dt }),
	}
	return edt
}

// resolveTier returns a downstream dispatch tier corresponding to the dispatch
// key of the specified request. If there is no such tier instance then it is
// created using the associated factory. If the tier exists but stopping at the
// moment due to inactivity timeout, then a successor tier instance is created
// and returned.
func (d *dispatcher) resolveTier(req consumeRequest) dispatchTier {
	childKey := d.factory.dispatchKey(req)
	edt := d.children[childKey]
	if edt == nil {
		edt = d.newExpiringDispatchTier(d.factory, childKey)
		d.children[childKey] = edt
	}
	if !edt.expired && edt.timer.Reset(edt.d.cfg.Consumer.RegistrationTimeout) {
		return edt.instance
	}
	if edt.successor == nil {
		edt.successor = edt.factory.newDispatchTier(edt.instance.key())
	}
	return edt.successor
}

// handleExpired marks the respective dispatch tier as expired and triggers its
// asynchronous stop. When the tier is stopped it will notify about that via the
// `stoppedChildrenCh` channel.
func (d *dispatcher) handleExpired(dt dispatchTier) {
	log.Infof("<%s> child expired: %s", d.contextID, dt)
	edt := d.children[dt.key()]
	if edt == nil || edt.instance != dt || edt.expired {
		return
	}
	edt.expired = true
	go edt.instance.stop()
}

// handleStopped if the specified dispatch tier has a successor then it is
// started and takes over the tier's spot among the downstream dispatch tiers,
// otherwise the tier is deleted.
func (d *dispatcher) handleStopped(dt dispatchTier) dispatchTier {
	log.Infof("<%s> child stopped: %s", d.contextID, dt)
	edt := d.children[dt.key()]
	if edt == nil {
		return nil
	}
	successor := edt.successor
	if successor == nil {
		delete(d.children, dt.key())
		return nil
	}
	log.Infof("<%s> starting successor: %s", d.contextID, successor)
	edt.expired = false
	edt.instance = successor
	edt.successor = nil
	successor.start(edt.d.stoppedChildrenCh)
	timeout := edt.d.cfg.Consumer.RegistrationTimeout
	edt.timer = time.AfterFunc(timeout, func() { edt.d.expiredChildrenCh <- successor })
	return edt.instance
}
