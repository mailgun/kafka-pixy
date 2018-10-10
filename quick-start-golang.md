To use Kafka-Pixy from a Golang application you need to:

1. Install dependencies:

    ```bash
    go get -u github.com/mailgun/kafka-pixy
    go get -u github.com/pkg/errors
    go get -u google.golang.org/grpc
    ```

2. Add imports:

    ```go
    import (
        pb "github.com/mailgun/kafka-pixy/gen/golang"
        "github.com/pkg/errors"
        "google.golang.org/grpc"
        "google.golang.org/grpc/codes"
        "google.golang.org/grpc/status"
    )
    ```

3. Create a Kafka-Pixy client:

    ```go
    // Kafka-Pixy is supposed to be running on the same host hence 127.0.0.1,
    // and by default it listens on port 19091.
	conn, _ := grpc.Dial("127.0.0.1:19091", grpc.WithInsecure())
    // The client is thread safe so you need only one.
	client := pb.NewKafkaPixyClient(conn)
    ```
    
4. To produce message write something like this:

    ```go
    // Make sure the context does not timeout earlier then 
    // (producer.flush_frequency + producer.retry_backoff) * producer.retry_max
    // as configured in the Kafka-Pixy config file.
    rs, err := _kp_clt.Produce(ctx, &pb.ProdRq{
 	    topic: topic, key_value: key, message: msg})
    if err != nil {
        panic(err)
    }
    fmt.Printf("Produced: partition=%s, offset=%s\n", rs.Partition, rs.Offset)
    ```
    
5. To consume messages you need to run a consume-n-ack loop in a goroutine.
 It is usually started on application startup, and runs until the application
 is terminated. To increase throughput you can run several consume-n-ack
 goroutines, but the exact number should be selected based on performance
 measurements in each particular case.
    
    ```go
    // Runs consume-n-ack loop until context is done. Note that inner gRPC calls
    // do not use provided context, that is intentional because we want for the
    // current request to finish gracefully. Otherwise previously consumed message
    // may not be properly acknowledged and will be consumed again.
    func RunConsumeNAck(ctx context.Context, group, topic string, msgHandler func(msg []byte)) error {
        // Consume first message.
        var rs *pb.ConsRs
        var err error
        for {
            select {
            case <-ctx.Done():
                return nil
            default:
            }
            rs, err = client.ConsumeNAck(context.TODO(), &pb.ConsNAckRq{
                Topic: topic,
                Group: group,
                NoAck: true,
            })
            if err != nil {
                if status.Code(err) == codes.NotFound {
                    continue
                }
                return errors.Wrap(err, "while consuming first")
            }
            break
        }
        msgHandler(rs.Message)
        // Run consume+ack loop.
        ackPartition := rs.Partition
        ackOffset := rs.Offset
        for {
            select {
            case <-ctx.Done():
                return nil
            default:
            }
            rs, err = client.ConsumeNAck(context.TODO(), &pb.ConsNAckRq{
                Topic:        topic,
                Group:        group,
                AckPartition: ackPartition,
                AckOffset:    ackOffset,
            })
            if err != nil {
                if status.Code(err) == codes.NotFound {
                    continue
                }
                return errors.Wrap(err, "while consuming")
            }
            ackPartition = rs.Partition
            ackOffset = rs.Offset
            msgHandler(rs.Message)
        }
        // Ack the last consumed message.
        _, err = client.Ack(context.TODO(), &pb.AckRq{
            Topic:     topic,
            Group:     group,
            Partition: ackPartition,
            Offset:    ackOffset,
        })
        if err != nil {
            return errors.Wrapf(err, "while acking last")
        }
        return nil
    }
    ``` 
