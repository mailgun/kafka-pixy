package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"strconv"
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
	grpcAddr string
	topic    string
	isSync   bool
	threads  int
	count    int
	size     int
)

func init() {
	flag.StringVar(&grpcAddr, "addr", "localhost:19091", "gRPC server address")
	flag.StringVar(&topic, "topic", "test", "name of the topic")
	flag.BoolVar(&isSync, "sync", false, "should production be synchronous")
	flag.IntVar(&threads, "threads", 1, "number of concurrent producer threads")
	flag.IntVar(&count, "count", 10000, "number of messages to produce by all threads")
	flag.IntVar(&size, "size", 1000, "message size in bytes")
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

	fmt.Printf("Producing...")
	begin := time.Now()

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
					req := pb.ProdRq{
						Topic:     topic,
						KeyValue:  []byte(strconv.Itoa(i)),
						Message:   msg,
						AsyncMode: !isSync,
					}
					_, err := clt.Produce(context.Background(), &req)
					if err != nil {
						panic(errors.Wrapf(err, "failed to produce: thread=%d, no=%d", tid, i-messageIndexBegin))
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

	totalProgress := 0
	for progress := range progressCh {
		totalProgress += progress
		took := time.Now().Sub(begin)
		tookSec := float64(took) / float64(time.Second)
		fmt.Printf("\rProducing... %d/%d for %s at %dmsg(%s)/sec",
			totalProgress, count, took, int64(float64(totalProgress)/tookSec),
			prettyfmt.Bytes(int64(float64(size*totalProgress)/tookSec)))
	}
	took := time.Now().Sub(begin)
	tookSec := float64(took) / float64(time.Second)
	fmt.Printf("\rProduced %d messages of size %d for %s at %dmsg(%s)/sec\n",
		totalProgress, size, took, int64(float64(totalProgress)/tookSec),
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
