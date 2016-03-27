package consumer

import (
	"reflect"
	"sync"

	"github.com/mailgun/kafka-pixy/context"
)

// multiplexer pulls messages fetched by exclusive consumers and offers them
// one by one to the topic consumer choosing wisely between different exclusive
// consumers to ensure that none of them is neglected.
type multiplexer struct {
	contextID    *context.ID
	inputs       []muxInput
	output       muxOutput
	lastInputIdx int
	stopCh       chan none
	wg           sync.WaitGroup
}

type muxInput interface {
	messages() <-chan *ConsumerMessage
	acks() chan<- *ConsumerMessage
}

type muxOutput interface {
	messages() chan<- *ConsumerMessage
}

func spawnMultiplexer(baseCID *context.ID, output muxOutput, inputs []muxInput) *multiplexer {
	m := &multiplexer{
		contextID: baseCID.NewChild("mux"),
		inputs:    inputs,
		output:    output,
		stopCh:    make(chan none),
	}
	spawn(&m.wg, m.run)
	return m
}

func (m *multiplexer) run() {
	defer m.contextID.LogScope()()
	inputCount := len(m.inputs)
	// Prepare a list of reflective select cases that is used when there are no
	// messages available from any of the inputs and we need to wait on all
	// of them for the first message to be fetched. Yes, reflection is slow but
	// it is only used in a corner case.
	selectCases := make([]reflect.SelectCase, inputCount+1)
	for i, ec := range m.inputs {
		selectCases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ec.messages())}
	}
	selectCases[inputCount] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(m.stopCh)}

	nextMessages := make([]*ConsumerMessage, inputCount)
	inputIdx := -1
	for {
		// Collect next messages from all inputs.
		isAtLeastOneAvailable := false
		for i, msg := range nextMessages {
			if msg != nil {
				isAtLeastOneAvailable = true
				continue
			}
			select {
			case msg := <-m.inputs[i].messages():
				nextMessages[i] = msg
				isAtLeastOneAvailable = true
			default:
			}
		}
		// If none of the inputs has a message available, then wait until some
		// of them does or a stop signal is received.
		if !isAtLeastOneAvailable {
			selected, value, ok := reflect.Select(selectCases)
			// There is no need to check what particular channel is closed, for
			// only `stopCh` channel is ever gets closed.
			if !ok {
				return
			}
			nextMessages[selected] = value.Interface().(*ConsumerMessage)
		}
		// At this point there is at least one next message available.
		inputIdx = selectInput(inputIdx, nextMessages)
		// wait for read or stop
		select {
		case <-m.stopCh:
			return
		case m.output.messages() <- nextMessages[inputIdx]:
			m.inputs[inputIdx].acks() <- nextMessages[inputIdx]
			nextMessages[inputIdx] = nil
		}
	}
}

func (m *multiplexer) stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// selectInput picks an input that should be multiplexed next. It prefers the
// inputs with the largest lag. If there is more then one input with the largest
// lag then it picks the one that index is following the lastInputIdx.
func selectInput(lastInputIdx int, inputMessages []*ConsumerMessage) int {
	maxLag, maxLagIdx, maxLagCount := findMaxLag(inputMessages)
	if maxLagCount == 1 {
		return maxLagIdx
	}
	inputCount := len(inputMessages)
	for i := 1; i < inputCount; i++ {
		maxLagIdx = (lastInputIdx + i) % inputCount
		msg := inputMessages[maxLagIdx]
		if msg == nil {
			continue
		}
		inputLag := msg.HighWaterMark - msg.Offset
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
func findMaxLag(inputMessages []*ConsumerMessage) (maxLag int64, maxLagIdx, maxLagCount int) {
	maxLag = -1
	maxLagIdx = -1
	for i, msg := range inputMessages {
		if msg == nil {
			continue
		}
		inputLag := msg.HighWaterMark - msg.Offset
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
