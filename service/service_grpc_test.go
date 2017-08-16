package service

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/mailgun/kafka-pixy/config"
	pb "github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	. "gopkg.in/check.v1"
)

type ServiceGRPCSuite struct {
	cfg     *config.App
	kh      *kafkahelper.T
	cltConn *grpc.ClientConn
	clt     pb.KafkaPixyClient
}

var _ = Suite(&ServiceGRPCSuite{})

func (s *ServiceGRPCSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *ServiceGRPCSuite) SetUpTest(c *C) {
	s.cfg = &config.App{Proxies: make(map[string]*config.Proxy)}
	s.cfg.GRPCAddr = "127.0.0.1:19091"
	proxyCfg := testhelpers.NewTestProxyCfg("test_svc")
	s.cfg.Proxies["pxyG"] = proxyCfg
	s.cfg.DefaultCluster = "pxyG"

	var err error
	s.cltConn, err = grpc.Dial(s.cfg.GRPCAddr, grpc.WithInsecure())
	c.Assert(err, IsNil)
	s.clt = pb.NewKafkaPixyClient(s.cltConn)

	s.kh = kafkahelper.New(c)
}

func (s *ServiceGRPCSuite) TearDownTest(c *C) {
	s.cltConn.Close()
	s.kh.Close()
}

// If `key` is explicitly specified produced messages are deterministically
// distributed between partitions.
func (s *ServiceGRPCSuite) TestProduceWithKey(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// When
	for i := 0; i < 10; i++ {
		req := pb.ProdRq{
			Topic:     "test.4",
			KeyValue:  []byte(fmt.Sprintf("%d", i)),
			Message:   []byte("msg"),
			AsyncMode: true,
		}
		res, err := s.clt.Produce(ctx, &req, grpc.FailFast(false))
		c.Assert(err, IsNil)
		c.Assert(*res, Equals, pb.ProdRs{Partition: -1, Offset: -1})
	}
	// Stop service to make it commit asynchronously produced messages to Kafka.
	svc.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0]+3)
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1]+2)
	c.Assert(offsetsAfter[2], Equals, offsetsBefore[2]+2)
	c.Assert(offsetsAfter[3], Equals, offsetsBefore[3]+3)
}

// If `key` is undefined then a message is submitted to a random partition.
func (s *ServiceGRPCSuite) TestProduceKeyUndefined(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// When
	for i := 0; i < 100; i++ {
		req := pb.ProdRq{
			Topic:        "test.4",
			KeyUndefined: true,
			Message:      []byte("msg"),
			AsyncMode:    true,
		}
		res, err := s.clt.Produce(ctx, &req, grpc.FailFast(false))
		c.Assert(err, IsNil)
		c.Assert(*res, Equals, pb.ProdRs{Partition: -1, Offset: -1})
	}
	// Stop service to make it commit asynchronously produced messages to Kafka.
	svc.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then: each partition gets something
	c.Assert(offsetsAfter[0]-offsetsBefore[0], Not(Equals), 0)
	c.Assert(offsetsAfter[1]-offsetsBefore[1], Not(Equals), 0)
	c.Assert(offsetsAfter[2]-offsetsBefore[2], Not(Equals), 0)
	c.Assert(offsetsAfter[3]-offsetsBefore[3], Not(Equals), 0)
}

// If `key` is explicitly specified produced messages are deterministically
// distributed between partitions.
func (s *ServiceGRPCSuite) TestProduceDefaultKey(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// When
	for i := 0; i < 10; i++ {
		req := pb.ProdRq{
			Topic:     "test.4",
			Message:   []byte("msg"),
			AsyncMode: true,
		}
		res, err := s.clt.Produce(ctx, &req, grpc.FailFast(false))
		c.Assert(err, IsNil)
		c.Assert(*res, Equals, pb.ProdRs{Partition: -1, Offset: -1})
	}
	// Stop service to make it commit asynchronously produced messages to Kafka.
	svc.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0])
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1])
	c.Assert(offsetsAfter[2], Equals, offsetsBefore[2])
	c.Assert(offsetsAfter[3], Equals, offsetsBefore[3]+10)
}

// If a message is produced in synchronous mode then partition and offset
// returned in response are set to proper values.
func (s *ServiceGRPCSuite) TestProduceSync(c *C) {
	svc, err := Spawn(s.cfg)
	defer svc.Stop()
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// When
	req := pb.ProdRq{
		Topic:    "test.4",
		KeyValue: []byte("bar"),
		Message:  []byte("msg"),
	}
	res, err := s.clt.Produce(ctx, &req, grpc.FailFast(false))

	// Then
	c.Assert(err, IsNil)
	c.Assert(*res, Equals, pb.ProdRs{Partition: 2, Offset: offsetsBefore[2]})
}

func (s *ServiceGRPCSuite) TestProduceInvalidProxy(c *C) {
	svc, err := Spawn(s.cfg)
	defer svc.Stop()
	c.Assert(err, IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// When
	req := pb.ProdRq{
		Cluster:  "invalid",
		Topic:    "test.4",
		KeyValue: []byte("bar"),
		Message:  []byte("msg"),
	}
	res, err := s.clt.Produce(ctx, &req, grpc.FailFast(false))

	// Then
	c.Assert(grpc.ErrorDesc(err), Equals, "proxy `invalid` does not exist")
	c.Assert(grpc.Code(err), Equals, codes.InvalidArgument)
	c.Assert(res, IsNil)
}

// Offsets of messages consumed in auto-ack mode are properly committed.
func (s *ServiceGRPCSuite) TestConsumeAutoAck(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	s.kh.ResetOffsets("foo", "test.4")
	produced := s.kh.PutMessages("auto-ack", "test.4", map[string]int{"A": 17, "B": 19, "C": 23, "D": 29})
	consumed := make(map[string][]*pb.ConsRs)
	offsetsBefore := s.kh.GetCommittedOffsets("foo", "test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// When
	for i := 0; i < 88; i++ {
		req := pb.ConsNAckRq{
			Topic:   "test.4",
			Group:   "foo",
			AutoAck: true,
		}
		res, err := s.clt.ConsumeNAck(ctx, &req)
		c.Assert(err, IsNil, Commentf("failed to consume message #%d", i))
		key := string(res.KeyValue)
		consumed[key] = append(consumed[key], res)
	}
	svc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets("foo", "test.4")
	c.Assert(offsetsAfter[0].Val, Equals, offsetsBefore[0].Val+17)
	c.Assert(offsetsAfter[1].Val, Equals, offsetsBefore[1].Val+29)
	c.Assert(offsetsAfter[2].Val, Equals, offsetsBefore[2].Val+23)
	c.Assert(offsetsAfter[3].Val, Equals, offsetsBefore[3].Val+19)

	assertMsgs(c, consumed, produced)
}

// Offsets of messages consumed in auto-ack mode are properly committed.
func (s *ServiceGRPCSuite) TestGetOffsets(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	s.kh.ResetOffsets("foo", "test.4")
	s.kh.PutMessages("auto-ack", "test.4", map[string]int{"A": 1})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Get the offsets with a single message in topic
	res, err := s.clt.GetOffsets(ctx, &pb.GetOffsetsRq{Topic: "test.4", Group: "foo"})
	c.Assert(err, IsNil, Commentf("failed to get offsets"))
	c.Assert(res.Offsets[0].Lag, Equals, int64(1))
	c.Assert(res.Offsets[0].Count > 0, Equals, true)

	// Consume the message
	_, err = s.clt.ConsumeNAck(ctx, &pb.ConsNAckRq{Topic: "test.4", Group: "foo", AutoAck: true})
	c.Assert(err, IsNil, Commentf("failed to consume message"))

	// fetch offsets until the offset is committed
	for i := 0; i < 5; i++ {
		res, err = s.clt.GetOffsets(ctx, &pb.GetOffsetsRq{Topic: "test.4", Group: "foo"})
		c.Assert(err, IsNil, Commentf("failed to get offsets"))
		if res.Offsets[0].Lag == int64(0) {
			break
		}
		time.Sleep(time.Second)
	}

	// Verify the lag is zero
	c.Assert(res.Offsets[0].Lag, Equals, int64(0))
	c.Assert(res.Offsets[0].Count > 0, Equals, true)

	svc.Stop()
}

// This test shows how message consumption loop with explicit acks should look
// like.
func (s *ServiceGRPCSuite) TestConsumeExplicitAck(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	s.kh.ResetOffsets("foo", "test.4")
	produced := s.kh.PutMessages("explicit-ack", "test.4", map[string]int{"A": 17, "B": 19, "C": 23, "D": 29})
	consumed := make(map[string][]*pb.ConsRs)
	offsetsBefore := s.kh.GetCommittedOffsets("foo", "test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// When:

	// First message has to be consumed with NoAck set to true.
	req := pb.ConsNAckRq{
		Topic: "test.4",
		Group: "foo",
		NoAck: true,
	}
	res, err := s.clt.ConsumeNAck(ctx, &req)
	c.Assert(err, IsNil, Commentf("failed to consume first message"))
	key := string(res.KeyValue)
	consumed[key] = append(consumed[key], res)
	// Whenever a message is consumed previous one is acked.
	for i := 1; i < 88; i++ {
		req = pb.ConsNAckRq{
			Topic:        "test.4",
			Group:        "foo",
			AckPartition: res.Partition,
			AckOffset:    res.Offset,
		}
		res, err = s.clt.ConsumeNAck(ctx, &req)
		c.Assert(err, IsNil, Commentf("failed to consume message #%d", i))
		key := string(res.KeyValue)
		consumed[key] = append(consumed[key], res)
	}
	// Ack last message.
	ackReq := pb.AckRq{
		Topic:     "test.4",
		Group:     "foo",
		Partition: res.Partition,
		Offset:    res.Offset,
	}
	_, err = s.clt.Ack(ctx, &ackReq)
	c.Assert(err, IsNil, Commentf("failed ack last message"))

	svc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets("foo", "test.4")
	c.Assert(offsetsAfter[0].Val, Equals, offsetsBefore[0].Val+17)
	c.Assert(offsetsAfter[1].Val, Equals, offsetsBefore[1].Val+29)
	c.Assert(offsetsAfter[2].Val, Equals, offsetsBefore[2].Val+23)
	c.Assert(offsetsAfter[3].Val, Equals, offsetsBefore[3].Val+19)

	assertMsgs(c, consumed, produced)
}

func (s *ServiceGRPCSuite) TestConsumeExplicitProxy(c *C) {
	svc, err := Spawn(s.cfg)
	defer svc.Stop()
	c.Assert(err, IsNil)

	s.kh.ResetOffsets("foo", "test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	prodReq := pb.ProdRq{
		Topic:    "test.4",
		KeyValue: []byte("bar"),
		Message:  []byte(fmt.Sprintf("msg%d", rand.Int())),
	}
	prodRes, err := s.clt.Produce(ctx, &prodReq, grpc.FailFast(false))
	c.Assert(err, IsNil)

	// When
	consReq := pb.ConsNAckRq{Cluster: "pxyG", Topic: "test.4", Group: "foo"}
	consRes, err := s.clt.ConsumeNAck(ctx, &consReq)

	// Then
	c.Assert(err, IsNil)
	c.Assert(*consRes, DeepEquals, pb.ConsRs{
		Partition: prodRes.Partition,
		Offset:    prodRes.Offset,
		KeyValue:  prodReq.KeyValue,
		Message:   prodReq.Message,
	})
}

// When a message that was produced with undefined key is consumed, then
// KeyUndefined is set in the consume response.
func (s *ServiceGRPCSuite) TestConsumeKeyUndefined(c *C) {
	svc, err := Spawn(s.cfg)
	defer svc.Stop()
	c.Assert(err, IsNil)

	s.kh.ResetOffsets("foo", "test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	prodReq := pb.ProdRq{
		Topic:        "test.4",
		KeyUndefined: true,
		Message:      []byte(fmt.Sprintf("msg-%d", rand.Int())),
	}
	prodRes, err := s.clt.Produce(ctx, &prodReq, grpc.FailFast(false))
	c.Assert(err, IsNil)

	// When
	consReq := pb.ConsNAckRq{Topic: "test.4", Group: "foo"}
	consRes, err := s.clt.ConsumeNAck(ctx, &consReq)

	// Then
	c.Assert(err, IsNil)
	c.Assert(*consRes, DeepEquals, pb.ConsRs{
		Partition:    prodRes.Partition,
		Offset:       prodRes.Offset,
		KeyUndefined: true,
		Message:      prodReq.Message,
	})
}

func (s *ServiceGRPCSuite) TestConsumeInvalidProxy(c *C) {
	s.cfg.Proxies[s.cfg.DefaultCluster].Consumer.LongPollingTimeout = 100 * time.Millisecond
	svc, err := Spawn(s.cfg)
	defer svc.Stop()
	c.Assert(err, IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// When
	consReq := pb.ConsNAckRq{
		Cluster: "invalid",
		Topic:   fmt.Sprintf("non-existent-%d", rand.Int()),
		Group:   "foo"}
	consRes, err := s.clt.ConsumeNAck(ctx, &consReq, grpc.FailFast(false))

	// Then
	c.Assert(grpc.ErrorDesc(err), Equals, "proxy `invalid` does not exist")
	c.Assert(grpc.Code(err), Equals, codes.InvalidArgument)
	c.Assert(consRes, IsNil)
}
