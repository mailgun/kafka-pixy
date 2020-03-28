package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"sync"
	"time"

	pb "github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/mailgun/kafka-pixy/prettyfmt"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	reportingPeriod = 1 * time.Second
)

var (
	grpcAddr  string
	topic     string
	isSync    bool
	threads   int
	count     int
	size      int
	verbose   bool
	keyPrefix string
)

func init() {
	flag.StringVar(&grpcAddr, "addr", "localhost:19091", "gRPC server address")
	flag.StringVar(&topic, "topic", "test", "name of the topic")
	flag.BoolVar(&isSync, "sync", false, "should production be synchronous")
	flag.IntVar(&threads, "threads", 1, "number of concurrent producer threads")
	flag.IntVar(&count, "count", 10000, "number of messages to produce by all threads")
	flag.IntVar(&size, "size", 1000, "message size in bytes")
	flag.BoolVar(&verbose, "verbose", false, "print out every produced message")
	flag.StringVar(&keyPrefix, "prefix", "", "key prefix")
	flag.Parse()
}

func main() {
	msg := genmessage(size)
	progressCh := make(chan int)

	cltConn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		panic(errors.Wrap(err, "failed to dial gRPC server"))
	}
	clt := pb.NewKafkaPixyClient(cltConn)

	if !verbose {
		fmt.Printf("Producing...")
	}
	begin := time.Now()
	if keyPrefix == "" {
		keyPrefix = begin.Format(time.RFC3339)
	}

	go func() {
		var wg sync.WaitGroup
		chunkSize := count / threads
		for tid := 0; tid < threads; tid++ {
			tid := tid
			messageIndexBegin := chunkSize * tid
			messageIndexEnd := messageIndexBegin + chunkSize
			if count-messageIndexEnd < chunkSize {
				messageIndexEnd = count
			}
			wg.Add(1)
			go func() {
				defer wg.Done()

				recentProgress := 0
				checkpoint := time.Now()
				for i := messageIndexBegin; i < messageIndexEnd; i++ {
					key := fmt.Sprintf("%s_%d", keyPrefix, i)
					rq := pb.ProdRq{
						Topic:     topic,
						KeyValue:  []byte(key),
						Message:   msg,
						AsyncMode: !isSync,
					}
					rs, err := clt.Produce(context.Background(), &rq)
					if err != nil {
						panic(errors.Wrapf(err, "failed to produce: thread=%d, no=%d", tid, i-messageIndexBegin))
					}
					if verbose {
						fmt.Printf("Put: key=%s, partition=%d, offset=%d\n", key, rs.Partition, rs.Offset)
					}
					recentProgress += 1
					if time.Now().Sub(checkpoint) > reportingPeriod {
						progressCh <- recentProgress
						recentProgress = 0
						checkpoint = time.Now()
					}
				}
				progressCh <- recentProgress
			}()
		}
		wg.Wait()
		close(progressCh)
	}()

	progressFmt := "Producing... %d/%d for %s at %dmsg(%s)/sec"
	summaryFmt := "Produced %d messages of size %d for %s at %dmsg(%s)/sec\n"
	if verbose {
		progressFmt = progressFmt + "\n"
	} else {
		progressFmt = "\r" + progressFmt
		summaryFmt = "\r" + summaryFmt
	}

	totalProgress := 0
	for progress := range progressCh {
		totalProgress += progress
		took := time.Now().Sub(begin)
		tookSec := float64(took) / float64(time.Second)
		fmt.Printf(progressFmt, totalProgress, count, took, int64(float64(totalProgress)/tookSec),
			prettyfmt.Bytes(int64(float64(size*totalProgress)/tookSec)))
	}
	took := time.Now().Sub(begin)
	tookSec := float64(took) / float64(time.Second)
	fmt.Printf(summaryFmt, totalProgress, size, took, int64(float64(totalProgress)/tookSec),
		prettyfmt.Bytes(int64(float64(size*totalProgress)/tookSec)))
}

func genmessage(size int) []byte {
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		panic(errors.Wrap(err, "failed to generate message"))
	}
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	if n, err := encoder.Write(raw); err != nil || n != len(raw) {
		panic(errors.Wrap(err, "failed to encode message"))
	}
	encoded := buf.Bytes()
	if len(encoded) > size {
		return encoded[:size]
	}
	return encoded
}
