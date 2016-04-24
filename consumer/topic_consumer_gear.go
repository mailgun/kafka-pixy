package consumer

import (
	"sort"
	"sync"

	"github.com/mailgun/kafka-pixy/consumer/multiplexer"
)

// topicConsumerGear represents a set of actors that a consumer group maintains
// for each consumed topic.
type topicConsumerGear struct {
	multiplexer muxActor
	tc          *topicConsumer
	inputs      map[int32]muxInputActor

	// Exist just to be overridden in tests with mocks.
	spawnInputFn spawnInputFn
	spawnMuxFn   spawnMuxFn
}

type muxInputActor interface {
	multiplexer.In
	Stop()
}

type muxActor interface {
	Stop()
}

type spawnInputFn func(topic string, partition int32) muxInputActor

type spawnMuxFn func(output multiplexer.Out, inputs []multiplexer.In) muxActor

// newTopicConsumerGear makes a new `topicConsumerGear`.
func newTopicConsumerGear(spawnInputFn spawnInputFn) *topicConsumerGear {
	return &topicConsumerGear{
		inputs:       make(map[int32]muxInputActor),
		spawnInputFn: spawnInputFn,
		spawnMuxFn: func(output multiplexer.Out, inputs []multiplexer.In) muxActor {
			return multiplexer.Spawn(output.(*topicConsumer).actorID, output, inputs)
		},
	}
}

// isIdle returns `true` if the respective topic is not being multiplexed due
// to either no partitions assigned or topic consumer not exists.
func (tcg *topicConsumerGear) isIdle() bool {
	return tcg.multiplexer == nil
}

// stop makes all input actors and multiplexer stop. Note that the underlying
// topic consumer is not stopped here.
func (tcg *topicConsumerGear) stop() {
	tcg.muxInputs(nil, nil)
}

// muxInputs ensures that inputs of all assigned partitions are spawned and
// multiplexed to the topic consumer. It stops inputs for partitions that are
// no longer assigned, spawns inputs for newly assigned partitions, and
// restarts the multiplexer, if the topic consumer input set has changed.
func (tcg *topicConsumerGear) muxInputs(tc *topicConsumer, assigned []int32) {
	var wg sync.WaitGroup

	if tcg.tc != tc {
		if tcg.multiplexer != nil {
			tcg.multiplexer.Stop()
			tcg.multiplexer = nil
		}
		tcg.tc = tc
	}

	if tc == nil {
		for partition, input := range tcg.inputs {
			wg.Add(1)
			go func(input muxInputActor) {
				defer wg.Done()
				input.Stop()
			}(input)
			delete(tcg.inputs, partition)
		}
		wg.Wait()
		return
	}

	for partition, input := range tcg.inputs {
		if !hasPartition(partition, assigned) {
			if tcg.multiplexer != nil {
				tcg.multiplexer.Stop()
				tcg.multiplexer = nil
			}
			wg.Add(1)
			go func(input muxInputActor) {
				defer wg.Done()
				input.Stop()
			}(input)
			delete(tcg.inputs, partition)
		}
	}
	wg.Wait()

	for _, partition := range assigned {
		if _, ok := tcg.inputs[partition]; !ok {
			if tcg.multiplexer != nil {
				tcg.multiplexer.Stop()
				tcg.multiplexer = nil
			}
			input := tcg.spawnInputFn(tc.topic, partition)
			tcg.inputs[partition] = input
		}
	}
	if tcg.multiplexer == nil && len(tcg.inputs) > 0 {
		tcg.multiplexer = tcg.spawnMuxFn(tc, sortedInputs(tcg.inputs))
	}
}

// muxInputsAsync calls muxInputs in another goroutine.
func (tcg *topicConsumerGear) muxInputsAsync(wg *sync.WaitGroup, tc *topicConsumer, assigned []int32) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		tcg.muxInputs(tc, assigned)
	}()
}

// sortedInputs given a partition->input map returns a slice of all the inputs
// from the map sorted in ascending order of partition ids.
func sortedInputs(inputs map[int32]muxInputActor) []multiplexer.In {
	partitions := make([]int32, 0, len(inputs))
	for p := range inputs {
		partitions = append(partitions, p)
	}
	sort.Sort(Int32Slice(partitions))
	sorted := make([]multiplexer.In, 0, len(inputs))
	for _, p := range partitions {
		sorted = append(sorted, inputs[p])
	}
	return sorted
}

func hasPartition(partition int32, partitions []int32) bool {
	count := len(partitions)
	if count == 0 {
		return false
	}
	return partitions[0] <= partition && partition <= partitions[count-1]
}
