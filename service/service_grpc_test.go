package service

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/config"
	pb "github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	. "gopkg.in/check.v1"
)

type ServiceGRPCSuite struct {
	cfg      *config.App
	proxyCfg *config.Proxy
	kh       *kafkahelper.T
	cltConn  *grpc.ClientConn
	clt      pb.KafkaPixyClient
}

var _ = Suite(&ServiceGRPCSuite{})

func (s *ServiceGRPCSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *ServiceGRPCSuite) SetUpTest(c *C) {
	s.cfg = &config.App{Proxies: make(map[string]*config.Proxy)}
	s.cfg.GRPCAddr = "127.0.0.1:19091"
	s.proxyCfg = testhelpers.NewTestProxyCfg("pxyG_client_id")
	s.proxyCfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	s.cfg.Proxies["pxyG"] = s.proxyCfg
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
	s.waitSvcUp(c, 5*time.Second)

	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		c.Check(err, IsNil)
		c.Check(*res, Equals, pb.ProdRs{Partition: -1, Offset: -1})
	}
	// Stop service to make it commit asynchronously produced messages to Kafka.
	svc.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then
	c.Check(offsetsAfter[0], Equals, offsetsBefore[0]+3)
	c.Check(offsetsAfter[1], Equals, offsetsBefore[1]+2)
	c.Check(offsetsAfter[2], Equals, offsetsBefore[2]+2)
	c.Check(offsetsAfter[3], Equals, offsetsBefore[3]+3)
}

// If `key` is undefined then a message is submitted to a random partition.
func (s *ServiceGRPCSuite) TestProduceKeyUndefined(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		c.Check(err, IsNil)
		c.Check(*res, Equals, pb.ProdRs{Partition: -1, Offset: -1})
	}
	// Stop service to make it commit asynchronously produced messages to Kafka.
	svc.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then: each partition gets something
	c.Check(offsetsAfter[0]-offsetsBefore[0], Not(Equals), 0)
	c.Check(offsetsAfter[1]-offsetsBefore[1], Not(Equals), 0)
	c.Check(offsetsAfter[2]-offsetsBefore[2], Not(Equals), 0)
	c.Check(offsetsAfter[3]-offsetsBefore[3], Not(Equals), 0)
}

// If `key` is explicitly specified produced messages are deterministically
// distributed between partitions.
func (s *ServiceGRPCSuite) TestProduceDefaultKey(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// When
	for i := 0; i < 10; i++ {
		req := pb.ProdRq{
			Topic:     "test.4",
			Message:   []byte("msg"),
			AsyncMode: true,
		}
		res, err := s.clt.Produce(ctx, &req, grpc.FailFast(false))
		c.Check(err, IsNil)
		c.Check(*res, Equals, pb.ProdRs{Partition: -1, Offset: -1})
	}
	// Stop service to make it commit asynchronously produced messages to Kafka.
	svc.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then
	c.Check(offsetsAfter[0], Equals, offsetsBefore[0])
	c.Check(offsetsAfter[1], Equals, offsetsBefore[1])
	c.Check(offsetsAfter[2], Equals, offsetsBefore[2])
	c.Check(offsetsAfter[3], Equals, offsetsBefore[3]+10)
}

// If a message is produced in synchronous mode then partition and offset
// returned in response are set to proper values.
func (s *ServiceGRPCSuite) TestProduceSync(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()
	s.waitSvcUp(c, 5*time.Second)

	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// When
	req := pb.ProdRq{
		Topic:    "test.4",
		KeyValue: []byte("bar"),
		Message:  []byte("msg"),
	}
	res, err := s.clt.Produce(ctx, &req, grpc.FailFast(false))

	// Then
	c.Check(err, IsNil)
	c.Check(*res, Equals, pb.ProdRs{Partition: 2, Offset: offsetsBefore[2]})
}

func (s *ServiceGRPCSuite) TestProduceInvalidProxy(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()
	s.waitSvcUp(c, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	grpcStatus, ok := status.FromError(err)
	c.Check(ok, Equals, true)
	c.Check(grpcStatus.Message(), Equals, "proxy `invalid` does not exist")
	c.Check(grpcStatus.Code(), Equals, codes.InvalidArgument)
	c.Check(res, IsNil)
}

func (s *ServiceGRPCSuite) TestProduceHeadersUnsupported(c *C) {
	if s.proxyCfg.Kafka.Version.IsAtLeast(sarama.V0_11_0_0) {
		c.Skip("Headers are supported on new Kafka")
	}

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()
	s.waitSvcUp(c, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// When
	req := pb.ProdRq{
		Topic:    "test.4",
		KeyValue: []byte("bar"),
		Message:  []byte("msg"),
		Headers: []*pb.RecordHeader{
			{Key: "foo", Value: []byte("bar")},
		},
	}
	res, err := s.clt.Produce(ctx, &req, grpc.FailFast(false))

	// Then
	grpcStatus, ok := status.FromError(err)
	c.Check(ok, Equals, true)
	c.Check(grpcStatus.Message(), Matches, "headers are not supported with this version of Kafka.*")
	c.Check(grpcStatus.Code(), Equals, codes.InvalidArgument)
	c.Check(res, IsNil)
}

// Offsets of messages consumed in auto-ack mode are properly committed.
func (s *ServiceGRPCSuite) TestConsumeAutoAck(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	s.kh.ResetOffsets("foo", "test.4")
	produced := s.kh.PutMessages("auto-ack", "test.4", map[string]int{"A": 17, "B": 19, "C": 23, "D": 29})
	consumed := make(map[string][]*pb.ConsRs)
	offsetsBefore := s.kh.GetCommittedOffsets("foo", "test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// When
	for i := 0; i < 88; i++ {
		req := pb.ConsNAckRq{
			Topic:   "test.4",
			Group:   "foo",
			AutoAck: true,
		}
		res, err := s.clt.ConsumeNAck(ctx, &req)
		c.Check(err, IsNil, Commentf("failed to consume message #%d", i))
		key := string(res.KeyValue)
		consumed[key] = append(consumed[key], res)
	}
	svc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets("foo", "test.4")
	c.Check(offsetsAfter[0].Val, Equals, offsetsBefore[0].Val+17)
	c.Check(offsetsAfter[1].Val, Equals, offsetsBefore[1].Val+29)
	c.Check(offsetsAfter[2].Val, Equals, offsetsBefore[2].Val+23)
	c.Check(offsetsAfter[3].Val, Equals, offsetsBefore[3].Val+19)

	assertMsgs(c, consumed, produced)
}

// If message is consumed with noAck but is not explicitly acknowledged, then
// its offset is not committed.
func (s *ServiceGRPCSuite) TestConsumeNoAck(c *C) {
	s.proxyCfg.Consumer.AckTimeout = 500 * time.Millisecond
	s.proxyCfg.Consumer.SubscriptionTimeout = 500 * time.Millisecond
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	s.kh.ResetOffsets("foo", "test.1")
	s.kh.PutMessages("no-ack", "test.1", map[string]int{"A": 1})
	offsetsBefore := s.kh.GetCommittedOffsets("foo", "test.1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// When
	req := pb.ConsNAckRq{
		Topic: "test.1",
		Group: "foo",
		NoAck: true,
	}
	_, err = s.clt.ConsumeNAck(ctx, &req)
	c.Check(err, IsNil)
	svc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets("foo", "test.1")
	c.Check(offsetsAfter[0].Val, Equals, offsetsBefore[0].Val)
}

// Offsets of messages consumed in auto-ack mode are properly committed.
func (s *ServiceGRPCSuite) TestGetOffsets(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	s.kh.ResetOffsets("foo", "test.4")
	s.kh.PutMessages("auto-ack", "test.4", map[string]int{"A": 1})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Get the offsets with a single message in topic
	res, err := s.clt.GetOffsets(ctx, &pb.GetOffsetsRq{Topic: "test.4", Group: "foo"})
	c.Check(err, IsNil, Commentf("failed to get offsets"))
	c.Check(res.Offsets[0].Lag, Equals, int64(1))
	c.Check(res.Offsets[0].Count > 0, Equals, true)

	// Consume the message
	_, err = s.clt.ConsumeNAck(ctx, &pb.ConsNAckRq{Topic: "test.4", Group: "foo", AutoAck: true})
	c.Check(err, IsNil, Commentf("failed to consume message"))

	// fetch offsets until the offset is committed
	for i := 0; i < 5; i++ {
		res, err = s.clt.GetOffsets(ctx, &pb.GetOffsetsRq{Topic: "test.4", Group: "foo"})
		c.Check(err, IsNil, Commentf("failed to get offsets"))
		if res.Offsets[0].Lag == int64(0) {
			break
		}
		time.Sleep(time.Second)
	}

	// Verify the lag is zero
	c.Check(res.Offsets[0].Lag, Equals, int64(0))
	c.Check(res.Offsets[0].Count > 0, Equals, true)

	svc.Stop()
}

// This test shows how message consumption loop with explicit acks should look
// like.
func (s *ServiceGRPCSuite) TestConsumeExplicitAck(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	s.kh.ResetOffsets("foo", "test.4")
	produced := s.kh.PutMessages("explicit-ack", "test.4", map[string]int{"A": 17, "B": 19, "C": 23, "D": 29})
	consumed := make(map[string][]*pb.ConsRs)
	offsetsBefore := s.kh.GetCommittedOffsets("foo", "test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// When:

	// First message has to be consumed with NoAck set to true.
	req := pb.ConsNAckRq{
		Topic: "test.4",
		Group: "foo",
		NoAck: true,
	}
	res, err := s.clt.ConsumeNAck(ctx, &req)
	c.Check(err, IsNil, Commentf("failed to consume first message"))
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
		c.Check(err, IsNil, Commentf("failed to consume message #%d", i))
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
	c.Check(err, IsNil, Commentf("failed ack last message"))

	svc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets("foo", "test.4")
	c.Check(offsetsAfter[0].Val, Equals, offsetsBefore[0].Val+17)
	c.Check(offsetsAfter[1].Val, Equals, offsetsBefore[1].Val+29)
	c.Check(offsetsAfter[2].Val, Equals, offsetsBefore[2].Val+23)
	c.Check(offsetsAfter[3].Val, Equals, offsetsBefore[3].Val+19)

	assertMsgs(c, consumed, produced)
}

func (s *ServiceGRPCSuite) TestConsumeExplicitProxy(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()
	s.waitSvcUp(c, 5*time.Second)

	s.kh.ResetOffsets("foo", "test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prodReq := pb.ProdRq{
		Topic:    "test.4",
		KeyValue: []byte("bar"),
		Message:  []byte(fmt.Sprintf("msg%d", rand.Int())),
	}
	prodRes, err := s.clt.Produce(ctx, &prodReq, grpc.FailFast(false))
	c.Check(err, IsNil)

	// When
	consReq := pb.ConsNAckRq{Cluster: "pxyG", Topic: "test.4", Group: "foo"}
	consRes, err := s.clt.ConsumeNAck(ctx, &consReq)

	// Then
	c.Check(err, IsNil)
	c.Check(*consRes, DeepEquals, pb.ConsRs{
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
	c.Assert(err, IsNil)
	defer svc.Stop()
	s.waitSvcUp(c, 5*time.Second)

	s.kh.ResetOffsets("foo", "test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prodReq := pb.ProdRq{
		Topic:        "test.4",
		KeyUndefined: true,
		Message:      []byte(fmt.Sprintf("msg-%d", rand.Int())),
	}
	prodRes, err := s.clt.Produce(ctx, &prodReq, grpc.FailFast(false))
	c.Check(err, IsNil)

	// When
	consReq := pb.ConsNAckRq{Topic: "test.4", Group: "foo"}
	consRes, err := s.clt.ConsumeNAck(ctx, &consReq)

	// Then
	c.Check(err, IsNil)
	c.Check(*consRes, DeepEquals, pb.ConsRs{
		Partition:    prodRes.Partition,
		Offset:       prodRes.Offset,
		KeyUndefined: true,
		Message:      prodReq.Message,
	})
}

// When a message that was produced with headers is conusmed, the headers should
// be present
func (s *ServiceGRPCSuite) TestConsumeHeaders(c *C) {
	if !s.proxyCfg.Kafka.Version.IsAtLeast(sarama.V0_11_0_0) {
		c.Skip("Headers not supported before Kafka v0.11")
	}

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()
	s.waitSvcUp(c, 5*time.Second)

	s.kh.ResetOffsets("foo", "test.4")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prodReq := pb.ProdRq{
		Topic:        "test.4",
		KeyUndefined: true,
		Message:      []byte(fmt.Sprintf("msg-%d", rand.Int())),
		Headers: []*pb.RecordHeader{
			{Key: "foo", Value: []byte("bar")},
		},
	}
	prodRes, err := s.clt.Produce(ctx, &prodReq, grpc.FailFast(false))
	c.Check(err, IsNil)

	// When
	consReq := pb.ConsNAckRq{Topic: "test.4", Group: "foo"}
	consRes, err := s.clt.ConsumeNAck(ctx, &consReq)

	// Then
	c.Check(err, IsNil)
	c.Check(*consRes, DeepEquals, pb.ConsRs{
		Partition:    prodRes.Partition,
		Offset:       prodRes.Offset,
		KeyUndefined: true,
		Message:      prodReq.Message,
		Headers:      prodReq.Headers,
	})
}

func (s *ServiceGRPCSuite) TestConsumeInvalidProxy(c *C) {
	s.cfg.Proxies[s.cfg.DefaultCluster].Consumer.LongPollingTimeout = 100 * time.Millisecond
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()
	s.waitSvcUp(c, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// When
	consReq := pb.ConsNAckRq{
		Cluster: "invalid",
		Topic:   fmt.Sprintf("non-existent-%d", rand.Int()),
		Group:   "foo"}
	consRes, err := s.clt.ConsumeNAck(ctx, &consReq, grpc.FailFast(false))

	// Then
	grpcStatus, ok := status.FromError(err)
	c.Check(ok, Equals, true)
	c.Check(grpcStatus.Message(), Equals, "proxy `invalid` does not exist")
	c.Check(grpcStatus.Code(), Equals, codes.InvalidArgument)
	c.Check(consRes, IsNil)
}

func (s *ServiceGRPCSuite) TestConsumeDisabled(c *C) {
	s.proxyCfg.Consumer.Disabled = true
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()
	s.waitSvcUp(c, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// When
	req := pb.ConsNAckRq{
		Topic:   "test.4",
		Group:   "foo",
		AutoAck: true,
	}
	res, err := s.clt.ConsumeNAck(ctx, &req, grpc.FailFast(false))

	// Then
	grpcStatus, ok := status.FromError(err)
	c.Check(ok, Equals, true)
	c.Check(grpcStatus.Message(), Equals, "service is disabled by configuration")
	c.Check(grpcStatus.Code(), Equals, codes.Unavailable)
	c.Check(res, IsNil)
}

// When the last group member leaves, the group znode in ZooKeeper is deleted.
func (s *ServiceGRPCSuite) TestGroupZNodeDeleted(c *C) {
	s.kh.PutMessages("missing-topic", "test.4", map[string]int{"A": 1, "B": 1, "C": 1, "D": 1})

	svc1, clt1 := spawnGRPCSvc(c, 1, "m1")
	svc2, clt2 := spawnGRPCSvc(c, 2, "m2")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rq := &pb.ConsNAckRq{
		Topic:   "test.4",
		Group:   "g1",
		AutoAck: true,
	}
	_, err := clt1.ConsumeNAck(ctx, rq)
	c.Assert(err, IsNil)
	_, err = clt2.ConsumeNAck(ctx, rq)
	c.Assert(err, IsNil)

	_, err = clt1.ConsumeNAck(ctx, rq)
	c.Assert(err, IsNil)
	_, err = clt2.ConsumeNAck(ctx, rq)
	c.Assert(err, IsNil)

	for _, path := range []string{
		"/consumers",
		"/consumers/g1",
		"/consumers/g1/ids",
		"/consumers/g1/ids/m1",
		"/consumers/g1/ids/m2",
		"/consumers/g1/owners",
		"/consumers/g1/owners/test.4",
	} {
		_, _, err := s.kh.ZKConn().Get(path)
		c.Assert(err, IsNil, Commentf(path))
	}

	svc1.Stop()
	_, err = clt2.ConsumeNAck(ctx, rq)
	c.Assert(err, IsNil)

	_, _, err = s.kh.ZKConn().Get("/consumers/g1/ids/m1")
	c.Assert(err, Equals, zk.ErrNoNode)
	for _, path := range []string{
		"/consumers",
		"/consumers/g1",
		"/consumers/g1/ids",
		"/consumers/g1/ids/m2",
		"/consumers/g1/owners",
		"/consumers/g1/owners/test.4",
	} {
		_, _, err := s.kh.ZKConn().Get(path)
		c.Assert(err, IsNil, Commentf(path))
	}

	// When
	svc2.Stop()

	// Then
	_, _, err = s.kh.ZKConn().Get("/consumers/g1")
	c.Assert(err, Equals, zk.ErrNoNode)
}

func spawnGRPCSvc(c *C, index int, memberID string) (*T, pb.KafkaPixyClient) {
	cfg := &config.App{Proxies: make(map[string]*config.Proxy)}
	cfg.GRPCAddr = fmt.Sprintf("127.0.0.1:1910%d", index)
	proxyCfg := testhelpers.NewTestProxyCfg(memberID)
	proxyCfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	cfg.Proxies["default"] = proxyCfg
	cfg.DefaultCluster = "default"
	svc, err := Spawn(cfg)
	c.Assert(err, IsNil)

	cltConn, err := grpc.Dial(cfg.GRPCAddr, grpc.WithInsecure())
	c.Assert(err, IsNil)
	clt := pb.NewKafkaPixyClient(cltConn)
	return svc, clt
}

// Attempts to consume from a missing topic do not disrupt consumption from an
// existing one (Issue #54)[https://github.com/mailgun/kafka-pixy/issues/54].
func (s *ServiceGRPCSuite) TestConsumeMissingTopic(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	s.waitSvcUp(c, 5*time.Second)

	s.kh.ResetOffsets("foo", "test.1")
	produced := s.kh.PutMessages("missing-topic", "test.1", map[string]int{"A": 10})
	offsetsBefore := s.kh.GetCommittedOffsets("foo", "test.1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// When: attempts to consume from a missing continue.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = s.clt.ConsumeNAck(ctx, &pb.ConsNAckRq{
			Topic:   "missing",
			Group:   "foo",
			AutoAck: true,
		})
		c.Assert(status.Code(err), Equals, codes.NotFound, Commentf("failed to consume message #%d"))
		select {
		case <-ctx.Done():
			return
		default:
		}
	}()

	// Then: consumption from an existing topic is not disrupted.
	consumed := make(map[string][]*pb.ConsRs)
	for i := 0; i < len(produced["A"]); i++ {
		rs, err := s.clt.ConsumeNAck(ctx, &pb.ConsNAckRq{
			Topic:   "test.1",
			Group:   "foo",
			AutoAck: true,
		})
		c.Check(err, IsNil, Commentf("failed to consume message #%d", i))
		if err == nil {
			key := string(rs.KeyValue)
			consumed[key] = append(consumed[key], rs)
		}
	}
	svc.Stop()
	offsetsAfter := s.kh.GetCommittedOffsets("foo", "test.1")
	c.Check(offsetsAfter[0].Val, Equals, offsetsBefore[0].Val+int64(len(produced["A"])))
	assertMsgs(c, consumed, produced)

	// Cleanup
	cancel()
	wg.Wait()
}

func (s *ServiceGRPCSuite) waitSvcUp(c *C, timeout time.Duration) {
	start := time.Now()
	for {
		if _, err := s.clt.GetOffsets(context.Background(), &pb.GetOffsetsRq{Topic: "test.1", Group: "test"}); err == nil {
			return
		}
		if time.Now().Sub(start) > timeout {
			c.Errorf("Service is not up")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
