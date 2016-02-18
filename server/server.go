package server

import (
	"fmt"
	"github.com/anacrolix/utp"
	"github.com/scalaxy/scalaxy/proto"
	"github.com/zhenjl/sqlparser"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Server struct {
	listener   net.Listener
	grpcServer *grpc.Server
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Run() error {
	var err error
	s.listener, err = utp.NewSocket("udp", fmt.Sprintf("0.0.0.0:%d", 3150))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	s.grpcServer = grpc.NewServer()
	proto.RegisterClientServiceServer(s.grpcServer, &clientServiceImpl{})
	return s.grpcServer.Serve(s.listener)
}

type clientServiceImpl struct {
}

func (c *clientServiceImpl) Query(request *proto.QueryRequest,
	srv proto.ClientService_QueryServer) error {
	log.Printf("query: %s", request.Query)
	stmt, err := sqlparser.Parse(request.Query)
	if err != nil {
		return fmt.Errorf("sql parser err: %s", err)
	}
	log.Printf("stmt %s", stmt)
	for i := 0; i < 10; i++ {
		resp := &proto.QueryResponse{
			Columns: []*proto.Value{
				&proto.Value{
					Type: 0x01,
					Data: []byte("Test User"),
				},
				&proto.Value{
					Type: 0x02,
					Data: []byte("100.0"),
				},
			},
		}
		err := srv.Send(resp)
		if err != nil {
			return err
		}
	}
	return nil
}
