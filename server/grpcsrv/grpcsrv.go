package grpcsrv

import (
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	pb "github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/mailgun/kafka-pixy/proxy"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	maxRequestSize = 1 * 1024 * 1024 // 1Mb
)

type T struct {
	actorID  *actor.ID
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
		actorID:  actor.RootID.NewChild(fmt.Sprintf("grpc://%s", addr)),
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
	actor.Spawn(s.actorID, &s.wg, func() {
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

// Stop gracefully stops the gRPC server. It stops listening on the socket for
// incoming requests first, and then blocks waiting for pending requests to
// complete.
func (s *T) Stop() {
	s.grpcSrv.GracefulStop()
	s.wg.Wait()
	close(s.errorCh)
}

// Produce implements pb.KafkaPixyServer
func (s *T) Produce(ctx context.Context, req *pb.ProdRq) (*pb.ProdRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	if req.AsyncMode {
		pxy.AsyncProduce(req.Topic, keyEncoderFor(req), sarama.StringEncoder(req.Message))
		return &pb.ProdRs{Partition: -1, Offset: -1}, nil
	}

	prodMsg, err := pxy.Produce(req.Topic, keyEncoderFor(req), sarama.StringEncoder(req.Message))
	if err != nil {
		switch err {
		case sarama.ErrUnknownTopicOrPartition:
			return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
		default:
			return nil, grpc.Errorf(codes.Internal, err.Error())
		}
	}
	return &pb.ProdRs{Partition: prodMsg.Partition, Offset: prodMsg.Offset}, nil
}

// ConsumeNAck implements pb.KafkaPixyServer
func (s *T) ConsumeNAck(ctx context.Context, req *pb.ConsNAckRq) (*pb.ConsRs, error) {
	pxy, err := s.proxySet.Get(req.Cluster)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	var ack proxy.Ack
	if req.NoAck {
		ack = proxy.NoAck()
	} else if req.AutoAck {
		ack = proxy.AutoAck()
	} else {
		if ack, err = proxy.NewAck(req.AckPartition, req.AckOffset); err != nil {
			return nil, grpc.Errorf(codes.InvalidArgument, errors.Wrap(err, "invalid ack").Error())
		}
	}

	consMsg, err := pxy.Consume(req.Group, req.Topic, ack)
	if err != nil {
		switch err {
		case consumer.ErrRequestTimeout:
			return nil, grpc.Errorf(codes.NotFound, err.Error())
		case consumer.ErrTooManyRequests:
			return nil, grpc.Errorf(codes.ResourceExhausted, err.Error())
		default:
			return nil, grpc.Errorf(codes.Internal, err.Error())
		}
	}
	res := pb.ConsRs{
		Partition: consMsg.Partition,
		Offset:    consMsg.Offset,
		Message:   consMsg.Value,
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
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error())
	}

	ack, err := proxy.NewAck(req.Partition, req.Offset)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, errors.Wrap(err, "invalid ack").Error())
	}
	if err = pxy.Ack(req.Group, req.Topic, ack); err != nil {
		return nil, grpc.Errorf(codes.Code(http.StatusInternalServerError), err.Error())
	}
	return &pb.AckRs{}, nil
}

func keyEncoderFor(prodReq *pb.ProdRq) sarama.Encoder {
	if prodReq.KeyUndefined {
		return nil
	}
	return sarama.ByteEncoder(prodReq.KeyValue)
}
