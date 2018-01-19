package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/pkg/errors"
	"github.com/thrawn01/args"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var Version = "dev-build"

func main() {
	desc := args.Dedent(`CLI for kafka-pixy to consume or produce messages

	Examples:
	   Post a simple message from stdin
	   $ echo -n 'my-message' | kafka-pixy-cli produce my-topic

	   Consume messages from a topic and write the message to stdout
	   $ kafka-pixy-cli consume my-topic

	 Help:
	   For detailed help on produce
	   $ kafka-pixy-cli produce -h

	   For detailed help on consume
	   $ kafka-pixy-cli consume -h

	   For detailed help on offsets
	   $ kafka-pixy-cli offsets -h`)

	parser := args.NewParser(args.EnvPrefix("KAFKA_PIXY_"), args.Desc(desc, args.IsFormated))
	parser.AddOption("--endpoint").
		Alias("-e").
		Env("ENDPOINT").
		Default("localhost:19091").
		Help("kafka-pixy endpoint - http// and unix:// are accepted")

	parser.AddOption("--verbose").
		Alias("-v").
		IsTrue().
		Help("be verbose")

	parser.AddCommand("produce", ProduceEvents)
	parser.AddCommand("consume", ConsumeEvents)
	parser.AddCommand("offsets", Offsets)
	parser.AddCommand("version", func(_ *args.ArgParser, _ interface{}) (int, error) {
		fmt.Fprintf(os.Stdout, "Version: %s\n", Version)
		return 1, nil
	})

	opts := parser.ParseOrExit(nil)
	client, err := DialKafkaPixy(opts.String("endpoint"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "-- while connecting to '%s' - %s", opts.String("endpoint"), err)
		os.Exit(1)
	}

	retCode, err := parser.RunCommand(client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "-- %s", err)
	}
	os.Exit(retCode)
}

func Offsets(parser *args.ArgParser, cast interface{}) (int, error) {
	client := cast.(pb.KafkaPixyClient)
	var err error

	desc := args.Dedent(`Calculates the lag and topic size by pulling offsets for a topic and group

	Examples:
	   Get lag information about a specific topic and group
	   $ kafka-pixy-cli offsets my-topic -g my-group`)

	parser.SetDesc(desc)
	parser.AddArgument("topic").
		Required().
		Env("TOPIC").
		Help("topic to post the event too")

	parser.AddOption("--group").
		Alias("-g").
		Default("kafka-pixy-cli").
		Env("GROUP").
		Help("consumer group to read offsets from")

	parser.AddOption("--lag").
		Alias("-l").
		IsTrue().
		Help("print only the total lag and counts for all partitions")

	opts := parser.ParseSimple(nil)
	if opts == nil {
		return 1, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	resp, err := client.GetOffsets(ctx, &pb.GetOffsetsRq{
		Topic: opts.String("topic"),
		Group: opts.String("group"),
	})
	cancel()
	if err != nil {
		return 1, errors.Wrapf(err, "while calling GetOffsets()")
	}

	if opts.Bool("lag") {
		var lag, count int64
		for _, offset := range resp.Offsets {
			lag += offset.Lag
			count += offset.Count
		}

		offset := pb.PartitionOffset{
			Count: count,
			Lag:   lag,
		}

		data, err := json.MarshalIndent(offset, "", "")
		if err != nil {
			return 1, errors.Wrap(err, "during JSON marshal")
		}
		fmt.Println(string(data))
		return 0, nil
	}

	for _, offset := range resp.Offsets {
		data, err := json.MarshalIndent(offset, "", "")
		if err != nil {
			return 1, errors.Wrap(err, "during JSON marshal")
		}
		fmt.Println(string(data))
	}

	return 0, nil
}

func ProduceEvents(parser *args.ArgParser, cast interface{}) (int, error) {
	client := cast.(pb.KafkaPixyClient)
	var err error

	desc := args.Dedent(`post messages to kafka-pixy from file or stdin

	Examples:
	   Post a simple event message from stdin
	   $ echo -n 'Hello World' | kafka-pixy-cli produce my-topic

	   Post multiple messages separated \r (carriage return)
	   $ echo -n 'message-one\rmessage-two' | kafka-pixy-cli produce -t my-topic

	   Post an event from a file (can contain multiple events separated by \r)
	   $ kafka-pixy-cli produce my-messages.txt -t my-topic

	   Use environment variables to simplify generating alot of events
	   $ export KAFKA_PIXY_TOPIC=my-topic
	   $ export KAFKA_PIXY_KEY=my-key
	   $ export KAFKA_PIXY_ENDPOINT=http://mailgun-dev:19092
	   $ for i in $(seq 1 10); do
	        UUID=${uuid}
		echo "{\"uuid\":{\"id\":\"$UUID\"}, \"foo\":\"bar\"}" | kafka-pixy-cli produce
	   done`)

	parser.SetDesc(desc)
	parser.AddArgument("topic").
		Required().
		Env("TOPIC").
		Help("topic to post the event too")

	parser.AddOption("--count").
		Alias("-c").
		Default("1").
		Env("COUNT").
		Help("number of times to post the event")

	parser.AddOption("--key").
		Alias("-k").
		Default("key").
		Env("KEY").
		Help("key to post the event with")

	parser.AddOption("--sync").
		IsTrue().
		Alias("-s").
		Env("SYNC").
		Help("message is submitted synchronously")

	parser.AddArgument("event-file")

	opts := parser.ParseSimple(nil)
	if opts == nil {
		return 1, nil
	}

	if err != nil {
		return 1, errors.Wrap(err, "while instantiating kafka-pixy-cli object")
	}

	// If a file was provided
	if opts.IsSet("event-file") {
		reader, err := os.Open(opts.String("event-file"))
		if err != nil {
			return 1, errors.Wrapf(err, "while opening '%s'", opts.String("event-file"))
		}
		return sendEvents(client, opts, reader)
	}
	// if stdin has an open pipe
	if !args.IsCharDevice(os.Stdin) {
		parser.PrintHelp()
		os.Exit(1)
	}
	return sendEvents(client, opts, os.Stdin)
}

func sendEvents(client pb.KafkaPixyClient, opts *args.Options, source io.Reader) (int, error) {
	count := opts.Int("count")

	for reader := NewEventReader(source); ; {
		body, err := reader.ReadEvent()
		if err != nil {
			if err == io.EOF {
				return 0, nil
			}
			return 1, errors.Wrap(err, "while reading event")
		}
		ctx := context.Background()
		for i := 0; i < count; i++ {
			rq := pb.ProdRq{
				Topic:     opts.String("topic"),
				KeyValue:  []byte(opts.String("key")),
				Message:   body,
				AsyncMode: opts.Bool("sync"),
			}

			if !opts.IsSet("key") {
				rq.KeyUndefined = true
			}

			resp, err := client.Produce(ctx, &rq)
			if err != nil {
				return 1, fmt.Errorf("produce to '%s' failed with '%s'\n", opts.String("endpoint"), err)
			}
			if opts.Bool("sync") {
				fmt.Printf("Partition: %d Offset: %d\n", resp.Partition, resp.Offset)
			}
		}
	}
}

func ConsumeEvents(parser *args.ArgParser, cast interface{}) (int, error) {
	client := cast.(pb.KafkaPixyClient)

	desc := args.Dedent(`consume events from kafka-pixy and print them to stdout

	Examples:
	   Watch for events on a topic
	   $ kafka-pixy-cli consume my-topic -g my-group
	   message-one
	   message-two
	   ...`)

	parser.SetDesc(desc)
	parser.AddArgument("topic").
		Required().
		Env("TOPIC").
		Help("topic to consume event from")

	parser.AddOption("--group").
		Alias("-g").
		Default("kafka-pixy-cli").
		Env("GROUP").
		Help("consumer group we are in")

	parser.AddOption("--buffer").
		IsInt().
		Alias("-b").
		Default("0").
		Env("BUFFER").
		Help("how many events to buffer before consumed")

	opts := parser.ParseSimple(nil)
	if opts == nil {
		return 1, nil
	}

	// req acts as thread local for this loop
	req := pb.ConsNAckRq{
		Topic:   opts.String("topic"),
		Group:   opts.String("group"),
		AutoAck: true,
	}

	for {
		ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
		res, err := client.ConsumeNAck(ctx, &req)
		if err != nil {
			if grpc.Code(err) != codes.NotFound {
				fmt.Fprintf(os.Stderr, "-- consume from '%s' message failed with '%s'\n",
					opts.String("endpoint"), err)
			}
			time.Sleep(time.Second * 2)
			continue
		}
		req.AckPartition = res.Partition
		req.AckOffset = res.Offset
		fmt.Println(string(res.Message))
	}
}

func DialKafkaPixy(endpoint string) (pb.KafkaPixyClient, error) {
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "while dialing gRPC server")
	}
	return pb.NewKafkaPixyClient(conn), nil
}
