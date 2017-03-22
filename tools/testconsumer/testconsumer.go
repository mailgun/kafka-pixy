package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	pb "github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/mailgun/kafka-pixy/prettyfmt"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	reportingPeriod = 1 * time.Second
)

var (
	grpcAddr    string
	group       string
	topic       string
	threads     int
	count       int
	waitForMore bool
)

func init() {
	flag.StringVar(&grpcAddr, "addr", "localhost:19091", "gRPC server address")
	flag.StringVar(&group, "group", "test", "name of the consumer group")
	flag.StringVar(&topic, "topic", "test", "name of the topic")
	flag.IntVar(&threads, "threads", 1, "number of concurrent consumer threads")
	flag.IntVar(&count, "count", 10000, "number of messages to consume by all threads")
	flag.BoolVar(&waitForMore, "wait", false, "should wait for more messages when the end of the topic is reached")
	flag.Parse()
}

func main() {
	progressCh := make(chan int, 100*threads)

	cltConn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		panic(errors.Wrap(err, "failed to dial gRPC server"))
	}
	clt := pb.NewKafkaPixyClient(cltConn)

	go func() {
		var wg sync.WaitGroup
		chunkSize := count / threads
		for tid := 0; tid < threads; tid++ {
			tid := tid
			wg.Add(1)
			go func() {
				defer wg.Done()
				startedAt := time.Now()
				// Consume first message.
				req := pb.ConsNAckRq{
					Topic: topic,
					Group: group,
					NoAck: true,
				}
				var res *pb.ConsRs
				for {
					res, err = clt.ConsumeNAck(context.Background(), &req)
					if err != nil {
						if grpc.Code(err) == codes.NotFound && waitForMore {
							continue
						}
						panic(errors.Wrap(err, "failed to consume first"))
					}
					break
				}
				fmt.Printf("First message consumed: thread=%d, took=%v, res=%+v\n", tid, time.Now().Sub(startedAt), res)
				progressCh <- len(res.Message)
				// Run consume+ack loop.
				ackPartition := res.Partition
				ackOffset := res.Offset
				for i := 1; i < chunkSize; i++ {
					req := pb.ConsNAckRq{
						Topic:        topic,
						Group:        group,
						AckPartition: ackPartition,
						AckOffset:    ackOffset,
					}
					res, err = clt.ConsumeNAck(context.Background(), &req)
					if err != nil {
						if grpc.Code(err) == codes.NotFound && waitForMore {
							continue
						}
						panic(errors.Wrapf(err, "failed to consume: thread=%d, no=%d", tid, i))
					}
					ackPartition = res.Partition
					ackOffset = res.Offset
					progressCh <- len(res.Message)

				}
				// Ack the last consumed message.
				ackReq := pb.AckRq{
					Topic:     topic,
					Group:     group,
					Partition: ackPartition,
					Offset:    ackOffset,
				}
				_, err = clt.Ack(context.Background(), &ackReq)
				if err != nil {
					panic(errors.Wrapf(err, "failed to ack last: thread=%d", tid))
				}
			}()
		}
		wg.Wait()
		close(progressCh)
	}()

	begin := time.Now()
	checkpoint := begin
	var count, totalCount int
	var bytes, totalBytes int64
	for msgSize := range progressCh {
		count += 1
		bytes += int64(msgSize)
		now := time.Now()
		took := now.Sub(checkpoint)
		if now.Sub(checkpoint) > reportingPeriod {
			totalTook := now.Sub(begin)
			totalCount += count
			totalBytes += bytes
			tookSec := float64(took) / float64(time.Second)
			fmt.Printf("\rConsuming... %d(%s) for %s at %dmsg(%s)/sec             ",
				totalCount, prettyfmt.Bytes(totalBytes), totalTook,
				int64(float64(count)/tookSec),
				prettyfmt.Bytes(int64(float64(bytes)/tookSec)))
			count = 0
			bytes = 0
			checkpoint = now
		}
	}
	totalTook := time.Now().Sub(begin)
	totalTookSec := float64(totalTook) / float64(time.Second)
	fmt.Printf("\rConsumed %d(%s) for %s at %dmsg(%s)/sec             \n",
		totalCount, prettyfmt.Bytes(totalBytes), totalTook,
		int64(float64(totalCount)/totalTookSec),
		prettyfmt.Bytes(int64(float64(totalBytes)/totalTookSec)))
}
