package multiplexer

import (
	"reflect"
	"sort"
	"sync"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/none"
)

// T fetches messages from inputs and multiplexes them to the output, giving
// preferences to inputs with higher lag. Multiplexes assumes ownership over
// inputs in the sense that it decides when an new input instance needs to
// started, or the old one stopped.
type T struct {
	actorID       *actor.ID
	spawnInF      SpawnInF
	fetchedInputs map[int32]*fetchedInput
	output        Out
	isRunning     bool
	stopCh        chan none.T
	wg            sync.WaitGroup
}

// In defines an interface of a multiplexer input.
type In interface {
	// Messages returns channel that multiplexer receives messages from.
	Messages() <-chan *consumer.Message

	// Acks returns a channel that multiplexer sends a message that was pulled
	// from the `Messages()` channel of this input after the message has been
	// send to the output.
	Acks() chan<- *consumer.Message

	// Stop signals the input to stop and blocks waiting for its goroutines to
	// complete.
	Stop()
}

// Out defines an interface of multiplexer output.
type Out interface {
	// Messages returns channel that multiplexer sends messages to.
	Messages() chan<- *consumer.Message
}

// SpawnInF is a function type that is used by multiplexer to spawn inputs for
// assigned partitions during rewiring.
type SpawnInF func(partition int32) In

// New creates a new multiplexer instance.
func New(namespace *actor.ID, spawnInF SpawnInF) *T {
	return &T{
		actorID:       namespace.NewChild("mux"),
		fetchedInputs: make(map[int32]*fetchedInput),
		spawnInF:      spawnInF,
		stopCh:        make(chan none.T),
	}
}

// input represents a multiplexer input along with a message to be fetched from
// that input next.
type fetchedInput struct {
	in      In
	nextMsg *consumer.Message
}

// IsRunning returns `true` if multiplexer is running pumping events from the
// inputs to the output.
func (m *T) IsRunning() bool {
	return m.isRunning
}

// WireUp ensures that assigned inputs are spawned and multiplexed to the
// specified output. It stops inputs for partitions that are no longer
// assigned, spawns inputs for newly assigned partitions, and restarts the
// multiplexer, if either output or any of inputs has changed.
//
// The multiplexer may be stopped if either output or all inputs are removed.
//
// WARNING: do not ever pass (*T)(nil) in output, that will cause panic.
func (m *T) WireUp(output Out, assigned []int32) {
	var wg sync.WaitGroup

	if m.output != output {
		m.stopIfRunning()
		m.output = output
	}
	// If output is not provided, then stop all inputs and return.
	if output == nil {
		for partition, input := range m.fetchedInputs {
			wg.Add(1)
			go func(fin *fetchedInput) {
				defer wg.Done()
				fin.in.Stop()
			}(input)
			delete(m.fetchedInputs, partition)
		}
		wg.Wait()
		return
	}
	// Stop inputs that are not assigned anymore.
	for partition, fin := range m.fetchedInputs {
		if !hasPartition(partition, assigned) {
			m.stopIfRunning()
			wg.Add(1)
			go func(fin *fetchedInput) {
				defer wg.Done()
				fin.in.Stop()
			}(fin)
			delete(m.fetchedInputs, partition)
		}
	}
	wg.Wait()
	// Spawn newly assigned inputs, but stop multiplexer before spawning the
	// first input.
	for _, partition := range assigned {
		if _, ok := m.fetchedInputs[partition]; !ok {
			m.stopIfRunning()
			in := m.spawnInF(partition)
			m.fetchedInputs[partition] = &fetchedInput{in, nil}
		}
	}
	if !m.IsRunning() && len(m.fetchedInputs) > 0 {
		m.start()
	}
}

// Stop synchronously stops the multiplexer.
func (m *T) Stop() {
	m.WireUp(nil, nil)
}

func (m *T) start() {
	actor.Spawn(m.actorID, &m.wg, m.run)
	m.isRunning = true
}

func (m *T) stopIfRunning() {
	if m.isRunning {
		m.stopCh <- none.V
		m.wg.Wait()
		m.isRunning = false
	}
}

func (m *T) run() {
	sortedIns := makeSortedIns(m.fetchedInputs)
	inputCount := len(sortedIns)
	// Prepare a list of reflective select cases that is used when there are no
	// messages available from any of the inputs and we need to wait on all
	// of them for the first message to be fetched. Yes, reflection is slow but
	// it is only used in a corner case.
	selectCases := make([]reflect.SelectCase, inputCount+1)
	for i, input := range sortedIns {
		selectCases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(input.in.Messages())}
	}
	selectCases[inputCount] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(m.stopCh)}

	inputIdx := -1
	for {
		// Collect next messages from all inputs.
		isAtLeastOneAvailable := false
		for _, input := range sortedIns {
			if input.nextMsg != nil {
				isAtLeastOneAvailable = true
				continue
			}
			select {
			case msg := <-input.in.Messages():
				input.nextMsg = msg
				isAtLeastOneAvailable = true
			default:
			}
		}
		// If none of the inputs has a message available, then wait until some
		// of them does or a stop signal is received.
		if !isAtLeastOneAvailable {
			selected, value, _ := reflect.Select(selectCases)
			// Check if it is a stop signal.
			if selected == inputCount {
				return
			}
			sortedIns[selected].nextMsg = value.Interface().(*consumer.Message)
		}
		// At this point there is at least one next message available.
		inputIdx = selectInput(inputIdx, sortedIns)
		// Wait for read or a stop signal.
		select {
		case <-m.stopCh:
			return
		case m.output.Messages() <- sortedIns[inputIdx].nextMsg:
			sortedIns[inputIdx].in.Acks() <- sortedIns[inputIdx].nextMsg
			sortedIns[inputIdx].nextMsg = nil
		}
	}
}

// makeSortedIns given a partition->input map returns a slice of all the inputs
// from the map sorted in ascending order of partition ids.
func makeSortedIns(inputs map[int32]*fetchedInput) []*fetchedInput {
	partitions := make([]int32, 0, len(inputs))
	for p := range inputs {
		partitions = append(partitions, p)
	}
	sort.Sort(Int32Slice(partitions))
	sortedIns := make([]*fetchedInput, len(inputs))
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

// selectInput picks an input that should be multiplexed next. It prefers the
// inputs with the largest lag. If there is more then one input with the largest
// lag then it picks the one that index is following the lastInputIdx.
func selectInput(lastInputIdx int, sortedIns []*fetchedInput) int {
	maxLag, maxLagIdx, maxLagCount := findMaxLag(sortedIns)
	if maxLagCount == 1 {
		return maxLagIdx
	}
	inputCount := len(sortedIns)
	for i := 1; i < inputCount; i++ {
		maxLagIdx = (lastInputIdx + i) % inputCount
		input := sortedIns[maxLagIdx]
		if input.nextMsg == nil {
			continue
		}
		inputLag := input.nextMsg.HighWaterMark - input.nextMsg.Offset
		if inputLag == maxLag {
			break
		}
	}
	return maxLagIdx
}

// findMaxLag traverses though the specified messages ignoring nil ones and,
// returns the value of the max lag among them, along with the index of the
// first message with the max lag value and the total count of messages that
// have max lag.
func findMaxLag(sortedIns []*fetchedInput) (maxLag int64, maxLagIdx, maxLagCount int) {
	maxLag = -1
	maxLagIdx = -1
	for i, input := range sortedIns {
		if input.nextMsg == nil {
			continue
		}
		inputLag := input.nextMsg.HighWaterMark - input.nextMsg.Offset
		if inputLag > maxLag {
			maxLagIdx = i
			maxLag = inputLag
			maxLagCount = 1
		} else if inputLag == maxLag {
			maxLagCount += 1
		}
	}
	return maxLag, maxLagIdx, maxLagCount
}

type Int32Slice []int32

func (p Int32Slice) Len() int           { return len(p) }
func (p Int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
