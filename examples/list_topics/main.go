package main

import (
	"context"
	"time"

	"github.com/davecgh/go-spew/spew"
	pb "github.com/mailgun/kafka-pixy/gen/golang"
	"google.golang.org/grpc"
)

func init() {
	spew.Config.SortKeys = true
}

func main() {
	conn, err := grpc.Dial("127.0.0.1:19091", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := pb.NewKafkaPixyClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rs, err := client.ListTopics(ctx, &pb.ListTopicRq{})
	if err != nil {
		panic(err)
	}
	spew.Dump(rs.Topics)
}
