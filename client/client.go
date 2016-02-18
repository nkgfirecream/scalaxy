package client

import (
	"fmt"
	"github.com/anacrolix/utp"
	"github.com/scalaxy/scalaxy/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	conn       *grpc.ClientConn
	grpcClient proto.ClientServiceClient
}

func Connect(addrStr string) (*Client, error) {
	var host string
	var portStr string
	var port uint16
	var err error

	client := &Client{}
	if strings.Index(addrStr, ":") > -1 {
		host, portStr, err = net.SplitHostPort(addrStr)
		log.Printf("host %s, port %d, err %s", host, portStr, err)
		if err != nil {
			return nil, err
		}
		_port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return nil, err
		}
		port = uint16(_port)
	} else {
		host = addrStr
		port = 3150
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("connect to addr %s", addr)

	client.conn, err = grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		log.Printf("connect to utp %s, timeout %.2fs", addr, timeout.Seconds())
		u, err := utp.DialTimeout(addr, timeout)
		if err != nil {
			log.Printf("utp err: %s", err)
		}
		return u, err
	}))
	if err != nil {
		return nil, err
	}
	client.grpcClient = proto.NewClientServiceClient(client.conn)

	return client, err
}

func (c *Client) Query(q string, respChan chan *proto.QueryResponse) error {
	log.Printf("query: %s", q)
	request := &proto.QueryRequest{
		Query: q,
	}
	queryClient, err := c.grpcClient.Query(context.Background(), request)
	if err != nil {
		return err
	}
	go func() {
		for {
			response, err := queryClient.Recv()
			if err != nil {
				if err != io.EOF {
					log.Printf("err: %s", err)
				}
				break
			}
			respChan <- response
		}
		close(respChan)
	}()
	return nil
}
