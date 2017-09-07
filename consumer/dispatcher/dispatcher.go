package dispatcher

import (
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/pkg/errors"
)

var (
	rsTooManyRequests = consumer.Response{Err: consumer.ErrTooManyRequests}
	rsUnavailable     = consumer.Response{Err: consumer.ErrUnavailable}
)

// T dispatcher requests to child nodes based on the request key value
// determined by a factory. It creates child nodes on demand using the factory.
// Children are obliged to notify the dispatcher when they stop by calling
// ChildSpec.Dispose() function. If dispatcher sees that a stopped child still
// has requests to process it will create a successor child.
//
// A dispatcher can be a child of another dispatcher. To do that it needs to
// be spawned with child spec passed to a child factory by an upstream
// dispatcher.
//
// Only root dispatcher can be explicitly stopped, the rest should only stop
// if their requests channel is closed or their last child terminated.
type T struct {
	actDesc    *actor.Descriptor
	cfg        *config.Proxy
	factory    Factory
	requestsCh chan consumer.Request
	childSpec  *ChildSpec
	finalizer  func()
	children   map[Key]chan consumer.Request
	disposalCh chan Key
	stoppedCh  chan none.T
}

// Key uniquely identifies a child that should handle a particular request.
type Key string

// Factory defines an interface to create dispatcher children.
type Factory interface {
	// KeyOf returns a key of a child that should server the specified request.
	KeyOf(rq consumer.Request) Key

	// SpawnChild creates and starts a new child instance that should read and
	// handle requests from requestsCh. The requests sent down to the channel
	// by the dispatcher are guaranteed to have the specified key.
	//
	// If a child stops/dies for any reason (e.g. expired, fatally failed, or
	// explicitly ordered to stop) it must send itself down to disposalCh to
	// let the parent dispatcher know, that it no longer handles requests.
	//
	// A child must not close either of the provided channels, otherwise the
	// dispatcher will panic.
	SpawnChild(childSpec ChildSpec)
}

// ChildSpec is a data structure passed to Factory.SpawnChild.
type ChildSpec struct {
	key        Key
	requestsCh chan consumer.Request
	disposalCh chan<- Key
}

// NewChildSpec4Test creates a ChildSpec to be used in testing.
func NewChildSpec4Test(requests chan consumer.Request) ChildSpec {
	return ChildSpec{
		key:        "test",
		requestsCh: requests,
		disposalCh: make(chan Key),
	}
}

// Key returns a key value that all requests dispatched to the child's
// requests channel will have.
func (cs *ChildSpec) Key() Key {
	return cs.key
}

// Requests returns a channel that dispatcher will send requests for this
// child to. The child is supposed to read from this channel. If the parent
// dispatcher closes the channel it means that the child should stop.
func (cs *ChildSpec) Requests() <-chan consumer.Request {
	return cs.requestsCh
}

// Dispose is a function that should be called when all child operation
// goroutine are stopped and it is ready to be garbage collected.
func (cs *ChildSpec) Dispose() {
	cs.disposalCh <- cs.key
}

// option is a functional parameter type for Spawn function.
type option func(d *T)

// WithChildSpec provides an optional dispatcher child spec passed by an
// upstream dispatcher when creating a child. Using this option a hierarchy of
// dispatchers can be built.
func WithChildSpec(childSpec ChildSpec) option {
	return func(d *T) {
		d.childSpec = &childSpec
		d.requestsCh = childSpec.requestsCh
	}
}

// WithFinalizer if provided then this function should be called before the
// dispatcher terminates.
func WithFinalizer(finalizer func()) option {
	return func(d *T) {
		d.finalizer = finalizer
	}
}

// Spawn creates and starts a dispatcher instance with a particular child
// factory and a proxy config. If the created dispatcher is a child of an
// upstream dispatcher then it should be initialized with a child spec provided
// by the upstream dispatcher in Factory.SpawnChild call.
func Spawn(parentActDesc *actor.Descriptor, factory Factory, cfg *config.Proxy, options ...option) *T {
	d := &T{
		actDesc:    parentActDesc.NewChild("disp"),
		cfg:        cfg,
		factory:    factory,
		children:   make(map[Key]chan consumer.Request),
		disposalCh: make(chan Key, cfg.Consumer.ChannelBufferSize),
		stoppedCh:  make(chan none.T),
	}
	for _, option := range options {
		option(d)
	}
	if d.requestsCh == nil {
		d.requestsCh = make(chan consumer.Request, cfg.Consumer.ChannelBufferSize)
	}
	actor.Spawn(d.actDesc, nil, d.run)
	return d
}

// Stop terminates a running dispatcher. In a hierarchy of dispatchers only the
// root dispatcher can be explicitly stopped. An attempt to stop a downstream
// dispatcher will result in panic.
func (d *T) Stop() {
	if d.childSpec != nil {
		panic(errors.New("only root dispatcher can be explicitly stopped"))
	}
	close(d.requestsCh)
	<-d.stoppedCh
}

// Wait4Stop blocks until the dispatcher terminates. It is supposed to be used
// with downstream dispatchers (not root) that cannot be stopped explicitly.
func (d *T) Wait4Stop(timeout time.Duration) bool {
	select {
	case <-d.stoppedCh:
		return true
	case <-time.After(timeout):
	}
	return false
}

// Requests returns a channel to send requests to the dispatcher.
func (d *T) Requests() chan<- consumer.Request {
	return d.requestsCh
}

// run receives consume requests from the `Requests()` channel and dispatches
// them to downstream tiers based on request dispatch key.
func (d *T) run() {
	if d.childSpec != nil {
		defer d.childSpec.Dispose()
	}
	defer close(d.stoppedCh)
	for {
		select {
		case rq, ok := <-d.requestsCh:
			if !ok {
				d.actDesc.Log().Info("Shutting down")
				goto wrapup
			}
			key := d.factory.KeyOf(rq)
			childRequestsCh := d.children[key]
			// If there is no child for the key, then spawn one.
			if childRequestsCh == nil {
				childRequestsCh = make(chan consumer.Request, d.cfg.Consumer.ChannelBufferSize)
				d.actDesc.Log().Infof("Spawning child: key=%s", key)
				d.factory.SpawnChild(ChildSpec{
					key:        key,
					requestsCh: childRequestsCh,
					disposalCh: d.disposalCh,
				})
				d.children[key] = childRequestsCh
			}
			// If the requests buffer is full then either the callers are
			// pulling too aggressively or the Kafka is experiencing issues.
			// Either way we reject requests right away and callers are
			// expected to back off for awhile and repeat their request later.
			select {
			case childRequestsCh <- rq:
			default:
				rq.ResponseCh <- rsTooManyRequests
			}

		case key := <-d.disposalCh:
			childRequestsCh := d.children[key]
			if childRequestsCh == nil {
				d.actDesc.Log().Errorf("Unexpected child: key=%s", key)
				continue
			}
			// If there are still requests in the stopped child requests
			// channel then spawn a successor child to handle them.
			if len(childRequestsCh) > 0 {
				d.actDesc.Log().Infof("Spawning successor: key=%s", key)
				d.factory.SpawnChild(ChildSpec{
					key:        key,
					requestsCh: childRequestsCh,
					disposalCh: d.disposalCh,
				})
				continue
			}
			delete(d.children, key)
			d.actDesc.Log().Infof("Disposed of child: key=%s, left=%d", key, len(d.children))
			// For all but the root dispatcher, when the last child terminates,
			// the dispatcher should also terminate.
			if d.childSpec != nil && len(d.children) == 0 {
				goto finalize
			}
		}
	}
wrapup:
	// Signal children to stop and wait for them to do so.
	for _, childRequestsCh := range d.children {
		close(childRequestsCh)
	}
	for len(d.children) > 0 {
		key := <-d.disposalCh
		requestsCh := d.children[key]
		if requestsCh == nil {
			d.actDesc.Log().Errorf("Unexpected child: key=%s", key)
			continue
		}
		for rq := range requestsCh {
			rq.ResponseCh <- rsUnavailable
		}
		delete(d.children, key)
		d.actDesc.Log().Infof("Disposed of child: key=%s, left=%d", key, len(d.children))
	}
finalize:
	// And finally call finalizer (pun is not intended :)) if it is specified.
	if d.finalizer != nil {
		d.finalizer()
	}
}
