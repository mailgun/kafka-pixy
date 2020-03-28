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
	"google.golang.org/grpc/status"
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
	verbose     bool
)

func init() {
	flag.StringVar(&grpcAddr, "addr", "localhost:19091", "gRPC server address")
	flag.StringVar(&group, "group", "test", "name of the consumer group")
	flag.StringVar(&topic, "topic", "test", "name of the topic")
	flag.IntVar(&threads, "threads", 1, "number of concurrent consumer threads")
	flag.IntVar(&count, "count", 10000, "number of messages to consume by all threads")
	flag.BoolVar(&waitForMore, "wait", false, "should wait for more messages when the end of the topic is reached")
	flag.BoolVar(&verbose, "verbose", false, "print out every consumed message")
	flag.Parse()
}

func main() {
	progressCh := make(chan int, 100*threads)

	cltConn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		panic(errors.Wrap(err, "failed to dial gRPC server"))
	}
	clt := pb.NewKafkaPixyClient(cltConn)
	begin := time.Now()

	go func() {
		var wg sync.WaitGroup
		chunkSize := count / threads
		for tid := 0; tid < threads; tid++ {
			tid := tid
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Consume first message.
				req := pb.ConsNAckRq{
					Topic: topic,
					Group: group,
					NoAck: true,
				}
				var rs *pb.ConsRs
				for {
					rs, err = clt.ConsumeNAck(context.Background(), &req)
					if err != nil {
						if status.Code(err) == codes.NotFound {
							if waitForMore {
								continue
							}
							return
						}
						panic(errors.Wrap(err, "failed to consume first"))
					}
					if verbose {
						fmt.Printf("Got: key=%s, partition=%d, offset=%d\n", string(rs.KeyValue), rs.Partition, rs.Offset)
					}
					break
				}
				progressCh <- len(rs.Message)
				// Run consume+ack loop.
				ackPartition := rs.Partition
				ackOffset := rs.Offset
				for i := 1; i < chunkSize; i++ {
					rq := pb.ConsNAckRq{
						Topic:        topic,
						Group:        group,
						AckPartition: ackPartition,
						AckOffset:    ackOffset,
					}
					rs, err = clt.ConsumeNAck(context.Background(), &rq)
					if err != nil {
						if status.Code(err) == codes.NotFound {
							if waitForMore {
								continue
							}
							return
						}
						panic(errors.Wrapf(err, "failed to consume: thread=%d, no=%d", tid, i))
					}
					ackPartition = rs.Partition
					ackOffset = rs.Offset
					progressCh <- len(rs.Message)
					if verbose {
						fmt.Printf("Got: key=%s, partition=%d, offset=%d\n", string(rs.KeyValue), rs.Partition, rs.Offset)
					}
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

	progressFmt := "Consuming... %d(%s) for %s at %dmsg(%s)/sec             "
	summaryFmt := "Consumed %d(%s) for %s at %dmsg(%s)/sec             \n"
	if verbose {
		progressFmt = progressFmt + "\n"
	} else {
		progressFmt = "\r" + progressFmt
		summaryFmt = "\r" + summaryFmt
	}

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
			fmt.Printf(progressFmt, totalCount, prettyfmt.Bytes(totalBytes), totalTook,
				int64(float64(count)/tookSec), prettyfmt.Bytes(int64(float64(bytes)/tookSec)))
			count = 0
			bytes = 0
			checkpoint = now
		}
	}
	totalCount += count
	totalBytes += bytes
	totalTook := time.Now().Sub(begin)
	totalTookSec := float64(totalTook) / float64(time.Second)
	fmt.Printf(summaryFmt, totalCount, prettyfmt.Bytes(totalBytes), totalTook,
		int64(float64(totalCount)/totalTookSec), prettyfmt.Bytes(int64(float64(totalBytes)/totalTookSec)))
}
