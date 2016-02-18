package main

import (
	"bytes"
	"fmt"
	"github.com/abiosoft/ishell"
	"github.com/scalaxy/scalaxy/client"
	"github.com/scalaxy/scalaxy/proto"
	"log"
	"os"
	"os/signal"
	"os/user"
	"strings"
	"time"
)

func main() {
	cli := &_cli{}
	if err := cli.run(); err != nil {
		log.Fatal(err)
	}
}

type _cli struct {
	netClient *client.Client
	shell     *ishell.Shell
}

func (c *_cli) run() error {
	usr, err := user.Current()
	if err != nil {
		return err
	}
	c.shell = ishell.New()
	c.shell.Register("connect", c.connect)
	c.shell.RegisterGeneric(c.cmd)
	historyPath := fmt.Sprintf("%s/.scalaxy/history", usr.HomeDir)
	log.Printf("history path: %s", historyPath)
	if err := c.shell.SetHistoryPath(historyPath); err != nil {
		return err
	}
	c.shell.SetHomeHistoryPath(historyPath)
	chSignal := make(chan os.Signal, 1)
	signal.Notify(chSignal, os.Interrupt, os.Kill)
	go func() {
		<-chSignal
		log.Printf("exit...")
		c.shell.Start()
	}()
	c.shell.Start()
	return nil
}

func (c *_cli) cmd(args ...string) (string, error) {
	if c.netClient == nil {
		return "", fmt.Errorf("Client disconnected.")
	}
	buf := &bytes.Buffer{}
	if len(args) == 0 {
		return "", fmt.Errorf("Empty query.")
	}
	t := time.Now()
	respChan := make(chan *proto.QueryResponse, 10)
	err := c.netClient.Query(strings.Join(args, " "), respChan)
	if err != nil {
		return "", err
	}
	ts1 := time.Since(t)
	for resp := range respChan {
		fmt.Fprintf(buf, "response: %v\n", *resp)
	}
	ts2 := time.Since(t)
	fmt.Fprintf(buf, "time spent: first response %.3fms, total %.3fms", ts1.Seconds()*1e3, ts2.Seconds()*1e3)
	return buf.String(), nil
}

func (c *_cli) connect(args ...string) (string, error) {
	addr := "localhost:3150"
	if len(args) == 1 {
		addr = args[0]
	}
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "Connecting to %s\n", addr)
	var err error
	c.netClient, err = client.Connect(addr)
	if err != nil {
		return "", err
	}
	fmt.Fprint(buf, "Connected.\n")
	return buf.String(), nil
}
