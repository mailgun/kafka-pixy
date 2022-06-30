package multiplexer

import (
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/none"
)

// T fetches messages from inputs and multiplexes them to the output, giving
// preferences to inputs with higher lag. Multiplexer assumes ownership over
// inputs in the sense that it decides when an new input instance needs to be
// started, or the old one stopped.
type T struct {
	actDesc   *actor.Descriptor
	spawnInFn SpawnInFn
	inputs    map[int32]*input
	output    Out
	isRunning int64
	stopCh    chan none.T
	wg        sync.WaitGroup

	sortedInsMu sync.Mutex
	sortedIns   []*input
}

// In defines an interface of a multiplexer input.
type In interface {
	// Messages returns a channel that multiplexer receives messages from.
	// Read messages should NOT be considered as consumed by the input.
	Messages() <-chan consumer.Message

	// IsSafe2Stop returns true if stopping the input now will cause neither
	// loss nor duplication of messages for the clients, false otherwise.
	IsSafe2Stop() bool

	// Stop signals the input to stop and blocks waiting for its goroutines to
	// complete.
	Stop()
}

// Out defines an interface of multiplexer output.
type Out interface {
	// Messages returns channel that multiplexer sends messages to.
	Messages() chan<- consumer.Message
}

// SpawnInFn is a function type that is used by multiplexer to spawn inputs for
// assigned partitions during rewiring.
type SpawnInFn func(partition int32) In

// New creates a new multiplexer instance.
func New(parentActDesc *actor.Descriptor, spawnInFn SpawnInFn) *T {
	return &T{
		actDesc:   parentActDesc.NewChild("mux"),
		inputs:    make(map[int32]*input),
		spawnInFn: spawnInFn,
		stopCh:    make(chan none.T),
	}
}

// input represents a multiplexer input along with a message to be fetched from
// that input next.
type input struct {
	In
	partition int32
	msg       consumer.Message
	msgOk     bool
}

// IsRunning returns `true` if multiplexer is running pumping events from the
// inputs to the output.
func (m *T) IsRunning() bool {
	return atomic.LoadInt64(&m.isRunning) == 1
}

// IsSafe2Stop returns true if it is safe to stop all of the multiplexer
// inputs. If at least one of them is not safe to stop then false is returned.
func (m *T) IsSafe2Stop() bool {
	m.sortedInsMu.Lock()
	sortedIns := m.sortedIns
	m.sortedInsMu.Unlock()
	for _, in := range sortedIns {
		if !in.IsSafe2Stop() {
			return false
		}
	}
	return true
}

// WireUp ensures that assigned inputs are spawned and multiplexed to the
// specified output. It stops inputs for partitions that are no longer
// assigned, spawns inputs for newly assigned partitions. Multiplexing is
// stopped while rewiring is in progress.
//
// WARNING: do not ever pass (*T)(nil) in output, that will cause panic.
func (m *T) WireUp(output Out, assigned []int32) {
	var wg sync.WaitGroup

	// Stop multiplexer while rewiring is in progress to avoid data races.
	wiringBegin := time.Now()
	m.stopIfRunning()
	m.output = output

	// If output is not provided, then stop all inputs and return.
	if output == nil {
		for p, in := range m.inputs {
			wg.Add(1)
			go func(in *input) {
				defer wg.Done()
				in.Stop()
			}(in)
			delete(m.inputs, p)
		}
		wg.Wait()
		return
	}

	// Stop inputs that are not assigned anymore.
	for p, in := range m.inputs {
		if !hasPartition(p, assigned) {
			wg.Add(1)
			go func(in *input) {
				defer wg.Done()
				in.Stop()
			}(in)
			delete(m.inputs, p)
		}
	}

	// Spawn newly assigned inputs, but stop multiplexer before spawning the
	// first input.
	for _, p := range assigned {
		if _, ok := m.inputs[p]; !ok {
			m.inputs[p] = &input{In: m.spawnInFn(p), partition: p}
		}
	}
	m.refreshSortedIns()

	// Start multiplexer, but only if there are assigned inputs.
	if len(m.inputs) > 0 {
		m.start()
	}

	waitBegin := time.Now()
	wiringTook := waitBegin.Sub(wiringBegin)

	// Wait for stopping inputs to stop.
	wg.Wait()

	waitedFor := time.Since(waitBegin)
	m.actDesc.Log().Infof("Wiring completed: took=%v, waited=%v", wiringTook, waitedFor)
}

// Stop synchronously stops the multiplexer.
func (m *T) Stop() {
	m.WireUp(nil, nil)
}

func (m *T) start() {
	actor.Spawn(m.actDesc, &m.wg, m.run)
	atomic.StoreInt64(&m.isRunning, 1)
}

func (m *T) stopIfRunning() {
	if atomic.LoadInt64(&m.isRunning) == 1 {
		m.stopCh <- none.V
		m.wg.Wait()
		atomic.StoreInt64(&m.isRunning, 0)
	}
}

func (m *T) refreshSortedIns() {
	sortedIns := makeSortedIns(m.inputs)
	m.sortedInsMu.Lock()
	m.sortedIns = sortedIns
	m.sortedInsMu.Unlock()
}

func (m *T) run() {
reset:
	inputCount := len(m.inputs)
	if inputCount == 0 {
		return
	}
	// Prepare a list of reflective select cases. It is used when none of the
	// inputs has fetched messages and we need to wait on all of them. Yes,
	// reflection is slow, but it is only used when there is nothing to
	// consume anyway.
	selectCases := make([]reflect.SelectCase, inputCount+1)
	for i, in := range m.sortedIns {
		selectCases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(in.Messages())}
	}
	selectCases[inputCount] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(m.stopCh)}

	// NOTE: When Stop() or WireUp() occurs the current `msg` might not
	// be delivered before `case <-m.stopCh` is evaluated. In this case
	// we store the message in m.sortedIns[X].msg for possible delivery
	// on the next iteration after a `reset` has occurred.

	// TODO: It is possible that an unconfirmed message keep kafka-pixy from releasing a
	//   partition during a re-balance? I think this situation also existed with
	//   the previous version of this code, so... should be okay?
	//   It's possible that KP will not shutdown the multiplexer until all outstanding
	//   messages are acknowledged, so this might not be a problem at all?

	for {
		isAtLeastOneAvailable := false
		for _, in := range m.sortedIns {

			// If we have a message to deliver from a previous iteration,
			// and we were interrupted by a WireUp(), send that message first.
			if in.msgOk {
				isAtLeastOneAvailable = true

				select {
				case m.output.Messages() <- in.msg:
					in.msgOk = false
				case <-m.stopCh:
					return
				}
				continue
			}

			select {
			case msg, ok := <-in.Messages():
				// If a channel of an input is closed, then the input should be
				// removed from the list of multiplexed inputs.
				if !ok {
					delete(m.inputs, in.partition)
					m.refreshSortedIns()
					goto reset
				}
				isAtLeastOneAvailable = true

				select {
				case m.output.Messages() <- msg:
				case <-m.stopCh:
					// Store the message in case stopCh eval is due to a WireUp() call, and we
					// need to provide this message later
					in.msgOk = true
					in.msg = msg
					return
				}
			default:
			}
		}

		// If none of the inputs has a message available, then wait until
		// a message is fetched on any of them or a stop signal is received.
		if !isAtLeastOneAvailable {
			idx, value, _ := reflect.Select(selectCases)
			// Check if it is a stop signal.
			if idx == inputCount {
				return
			}

			// Block until the output reads the next message of the selected input
			// or a stop signal is received.
			msg := value.Interface().(consumer.Message)
			select {
			case m.output.Messages() <- msg:
			case <-m.stopCh:
				// Store the message in case stopCh eval is due to a WireUp() call, and we
				// need to provide this message later
				m.sortedIns[idx].msgOk = true
				m.sortedIns[idx].msg = msg
				return
			}
		}
	}
}

// makeSortedIns given a partition->input map returns a slice of all the inputs
// from the map sorted in ascending order of partition ids.
func makeSortedIns(inputs map[int32]*input) []*input {
	partitions := make([]int32, 0, len(inputs))
	for p := range inputs {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	sortedIns := make([]*input, len(inputs))
	for i, p := range partitions {
		sortedIns[i] = inputs[p]
	}
	return sortedIns
}

func hasPartition(partition int32, partitions []int32) bool {
	count := len(partitions)
	if count == 0 {
		return false
	}
	return partitions[0] <= partition && partition <= partitions[count-1]
}
