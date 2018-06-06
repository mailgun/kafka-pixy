package grpcsrv

import (
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/admin"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/offsettrk"
	"github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/proxy"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxRequestSize = 1 * 1024 * 1024 // 1Mb
)

type T struct {
	actDesc  *actor.Descriptor
	listener net.Listener
	grpcSrv  *grpc.Server
	proxySet *proxy.Set
	wg       sync.WaitGroup
	errorCh  chan error
}

// New creates a gRPC server instance.
func New(addr string, proxySet *proxy.Set) (*T, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create listener")
	}

	grpcSrv := grpc.NewServer(grpc.MaxMsgSize(maxRequestSize))
	s := T{
		actDesc:  actor.Root().NewChild(fmt.Sprintf("grpc://%s", addr)),
		listener: listener,
		grpcSrv:  grpcSrv,
		proxySet: proxySet,
		errorCh:  make(chan error, 1),
	}
	pb.RegisterKafkaPixyServer(grpcSrv, &s)
	return &s, nil
}

// Starts triggers asynchronous gRPC server start. If it fails then the error
// will be sent down to `ErrorCh()`.
func (s *T) Start() {
	actor.Spawn(s.actDesc, &s.wg, func() {
		if err := s.grpcSrv.Serve(s.listener); err != nil {
			s.errorCh <- errors.Wrap(err, "gRPC API listener failed")
		}
	})
}

// ErrorCh returns an output channel that HTTP server running in another
// goroutine will use if it stops with error if one occurs. The channel will be
// closed when the server is fully stopped due to an error or otherwise..
func (s *T) ErrorCh() <-chan error {
	return s.errorCh
}

// Stop immediately stops gRPC server. So it is caller's responsibility to make
// sure that all pending requests are completed.
func (s *T) Stop() {
	s.grpcSrv.Stop()
	s.wg.Wait()
	close(s.errorCh)
}

// Produce implements pb.KafkaPixyServer
func (s *T) Produce(ctx context.Context, req *pb.ProdRq) (*pb.ProdRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	headers := make([]sarama.RecordHeader, 0, len(req.Headers))
	for _, h := range req.Headers {
		if h == nil {
			continue
		}
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(h.Key),
			Value: h.Value,
		})
	}

	if req.AsyncMode {
		pxy.AsyncProduce(req.Topic, keyEncoderFor(req), sarama.StringEncoder(req.Message), headers)
		return &pb.ProdRs{Partition: -1, Offset: -1}, nil
	}

	prodMsg, err := pxy.Produce(req.Topic, keyEncoderFor(req), sarama.StringEncoder(req.Message), headers)
	if err != nil {
		switch err {
		case sarama.ErrUnknownTopicOrPartition:
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		case proxy.ErrDisabled:
			fallthrough
		case proxy.ErrUnavailable:
			return nil, status.Errorf(codes.Unavailable, err.Error())
		case proxy.ErrHeadersUnsupported:
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, err.Error())
		}
	}
	return &pb.ProdRs{Partition: prodMsg.Partition, Offset: prodMsg.Offset}, nil
}

// ConsumeNAck implements pb.KafkaPixyServer
func (s *T) ConsumeNAck(ctx context.Context, req *pb.ConsNAckRq) (*pb.ConsRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	var ack proxy.Ack
	if req.NoAck {
		ack = proxy.NoAck()
	} else if req.AutoAck {
		ack = proxy.AutoAck()
	} else {
		if ack, err = proxy.NewAck(req.AckPartition, req.AckOffset); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, errors.Wrap(err, "invalid ack").Error())
		}
	}

	consMsg, err := pxy.Consume(req.Group, req.Topic, ack)
	if err != nil {
		switch err {
		case consumer.ErrRequestTimeout:
			return nil, status.Errorf(codes.NotFound, err.Error())
		case consumer.ErrTooManyRequests:
			return nil, status.Errorf(codes.ResourceExhausted, err.Error())
		case consumer.ErrUnavailable:
			fallthrough
		case proxy.ErrDisabled:
			fallthrough
		case proxy.ErrUnavailable:
			return nil, status.Errorf(codes.Unavailable, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, err.Error())
		}
	}
	res := pb.ConsRs{
		Partition: consMsg.Partition,
		Offset:    consMsg.Offset,
		Message:   consMsg.Value,
	}
	for _, h := range consMsg.Headers {
		res.Headers = append(res.Headers, &pb.RecordHeader{
			Key:   string(h.Key),
			Value: h.Value,
		})
	}
	if consMsg.Key == nil {
		res.KeyUndefined = true
	} else {
		res.KeyValue = consMsg.Key
	}
	return &res, nil
}

func (s *T) Ack(ctx context.Context, req *pb.AckRq) (*pb.AckRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	ack, err := proxy.NewAck(req.Partition, req.Offset)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, errors.Wrap(err, "invalid ack").Error())
	}
	if err = pxy.Ack(req.Group, req.Topic, ack); err != nil {
		return nil, status.Errorf(codes.Code(http.StatusInternalServerError), err.Error())
	}
	return &pb.AckRs{}, nil
}

func (s *T) GetOffsets(ctx context.Context, req *pb.GetOffsetsRq) (*pb.GetOffsetsRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	partitionOffsets, err := pxy.GetGroupOffsets(req.Group, req.Topic)
	if err != nil {
		if errors.Cause(err) == sarama.ErrUnknownTopicOrPartition {
			return nil, status.Errorf(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Code(http.StatusInternalServerError), err.Error())
	}

	result := pb.GetOffsetsRs{}
	for _, po := range partitionOffsets {
		row := pb.PartitionOffset{
			Partition: po.Partition,
			Begin:     po.Begin,
			End:       po.End,
			Count:     po.End - po.Begin,
			Offset:    po.Offset,
		}
		if po.Offset == sarama.OffsetNewest {
			row.Lag = 0
		} else if po.Offset == sarama.OffsetOldest {
			row.Lag = po.End - po.Begin
		} else {
			row.Lag = po.End - po.Offset
		}
		row.Metadata = po.Metadata
		offset := offsetmgr.Offset{Val: po.Offset, Meta: po.Metadata}
		row.SparseAcks = offsettrk.SparseAcks2Str(offset)
		result.Offsets = append(result.Offsets, &row)
	}
	return &result, nil
}

func (s *T) SetOffsets(ctx context.Context, req *pb.SetOffsetsRq) (*pb.SetOffsetsRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	partitionOffsets := make([]admin.PartitionOffset, len(req.Offsets))
	for i, pov := range req.Offsets {
		partitionOffsets[i].Partition = pov.Partition
		partitionOffsets[i].Offset = pov.Offset
		partitionOffsets[i].Metadata = pov.Metadata
	}

	err = pxy.SetGroupOffsets(req.Group, req.Topic, partitionOffsets)
	if err != nil {
		if err = errors.Cause(err); err == sarama.ErrUnknownTopicOrPartition {
			return nil, status.Errorf(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Code(http.StatusInternalServerError), err.Error())
	}

	return &pb.SetOffsetsRs{}, nil
}

func (s *T) ListTopics(ctx context.Context, req *pb.ListTopicRq) (*pb.ListTopicRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tms, err := pxy.ListTopics(req.GetWithPartitions(), true)
	if err != nil {
		if errors.Cause(err) == zk.ErrNoNode {
			return nil, status.Errorf(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Code(http.StatusInternalServerError), err.Error())
	}

	var res pb.ListTopicRs

	res.Topics = make(map[string]*pb.GetTopicMetadataRs)
	for _, tm := range tms {
		var t pb.GetTopicMetadataRs
		t.Version = tm.Config.Version
		t.Config = tm.Config.Config

		if req.WithPartitions {
			for _, p := range tm.Partitions {
				entry := pb.PartitionMetadata{
					Partition: p.ID,
					Leader:    p.Leader,
					Replicas:  p.Replicas,
					Isr:       p.ISR,
				}
				t.Partitions = append(t.Partitions, &entry)
			}
		}
		res.Topics[tm.Topic] = &t
	}
	return &res, nil
}

func (s *T) ListConsumers(ctx context.Context, req *pb.ListConsumersRq) (*pb.ListConsumersRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	var groups map[string]map[string][]int32
	if req.Group == "" {
		groups, err = pxy.GetAllTopicConsumers(req.Topic)
		if err != nil {
			if errors.Cause(err) == zk.ErrNoNode {
				return nil, status.Errorf(codes.NotFound, err.Error())
			}
			return nil, status.Errorf(codes.Code(http.StatusInternalServerError), err.Error())
		}
	} else {
		groupConsumers, err := pxy.GetTopicConsumers(req.Group, req.Topic)
		if err != nil {
			if errors.Cause(err) == zk.ErrNoNode {
				return nil, status.Errorf(codes.NotFound, err.Error())
			}
			if _, ok := err.(admin.ErrInvalidParam); ok {
				return nil, status.Errorf(codes.NotFound, err.Error())
			}
		}
		groups = make(map[string]map[string][]int32)
		if len(groupConsumers) != 0 {
			groups[req.Group] = groupConsumers
		}
	}

	var res pb.ListConsumersRs
	res.Groups = make(map[string]*pb.ConsumerGroups, len(groups))

	for group, consumers := range groups {
		consGroup := new(pb.ConsumerGroups)
		consGroup.Consumers = make(map[string]*pb.ConsumerPartitions, len(consumers))
		for consumer, partitions := range consumers {
			consGroup.Consumers[consumer] = &pb.ConsumerPartitions{Partitions: partitions}
		}
		res.Groups[group] = consGroup
	}
	return &res, nil
}

func (s *T) GetTopicMetadata(ctx context.Context, req *pb.GetTopicMetadataRq) (*pb.GetTopicMetadataRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	tm, err := pxy.GetTopicMetadata(req.Topic, req.WithPartitions, true)
	if err != nil {
		if errors.Cause(err) == zk.ErrNoNode {
			return nil, status.Errorf(codes.NotFound, err.Error())
		}
		if errors.Cause(err) == sarama.ErrUnknownTopicOrPartition {
			return nil, status.Errorf(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Code(http.StatusInternalServerError), err.Error())
	}

	var res pb.GetTopicMetadataRs

	res.Version = tm.Config.Version
	res.Config = tm.Config.Config

	if req.WithPartitions {
		for _, p := range tm.Partitions {
			entry := pb.PartitionMetadata{
				Partition: p.ID,
				Leader:    p.Leader,
				Replicas:  p.Replicas,
				Isr:       p.ISR,
			}
			res.Partitions = append(res.Partitions, &entry)
		}
	}
	return &res, nil
}

func keyEncoderFor(prodReq *pb.ProdRq) sarama.Encoder {
	if prodReq.KeyUndefined {
		return nil
	}
	return sarama.ByteEncoder(prodReq.KeyValue)
}
