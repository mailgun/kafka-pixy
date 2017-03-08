package grpcsrv

import (
	"fmt"
	"net"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	pb "github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/mailgun/kafka-pixy/proxy"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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
func (s *T) Produce(ctx context.Context, req *pb.ProdReq) (*pb.ProdRes, error) {
	pxy, err := s.proxySet.Get(req.Proxy)
	if err != nil {
		return nil, err
	}

	if req.AsyncMode {
		pxy.AsyncProduce(req.Topic, keyEncoderFor(req), sarama.StringEncoder(req.Message))
		return &pb.ProdRes{Partition: -1, Offset: -1}, nil
	}

	prodMsg, err := pxy.Produce(req.Topic, keyEncoderFor(req), sarama.StringEncoder(req.Message))
	if err != nil {
		return nil, err
	}
	return &pb.ProdRes{Partition: prodMsg.Partition, Offset: prodMsg.Offset}, nil
}

// Consume implements pb.KafkaPixyServer
func (s *T) Consume(ctx context.Context, req *pb.ConsReq) (*pb.ConsRes, error) {
	pxy, err := s.proxySet.Get(req.Proxy)
	if err != nil {
		return nil, err
	}

	consMsg, err := pxy.Consume(req.Group, req.Topic, proxy.NoAck())
	if err != nil {
		return nil, err
	}

	res := pb.ConsRes{
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

func keyEncoderFor(prodReq *pb.ProdReq) sarama.Encoder {
	if prodReq.KeyUndefined {
		return nil
	}
	return sarama.ByteEncoder(prodReq.KeyValue)
}
