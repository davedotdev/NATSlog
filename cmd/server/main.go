package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	nlogpb "github.com/davedotdev/proto/natslog/proto_natslog"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
)

const constREQDelay = 2 * time.Second

// Config used to decode configuration file
type Config struct {
	NATSHost    string `toml:"natshost"`
	NATSSubject string `toml:"nats_subject"`
	NATSName    string `toml:"nats_name"`
	Seq         int    `toml:"nats_start_sequence"`
	All         bool   `toml:"deliver_all"`
	Last        bool   `toml:"deliver_last"`
	Since       string `toml:"deliver_since"`
}

func getconfig(name string) (C Config, err error) {
	c := Config{}
	_, err = toml.DecodeFile(name, &c)

	if err != nil {
		return c, err
	}

	return c, nil
}

func setupNATSConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		log.Printf("Disconnected: will attempt reconnects for %.0fm", totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}

// func sends message to NATS
func sendLogToNATS(c Config, m nlogpb.Log) error {
	// Connect Options.
	opts := []nats.Option{nats.Name(c.NATSName)}

	// Connect to NATS
	nc, err := nats.Connect(c.NATSHost, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	pb, err := proto.Marshal(&m)
	if err != nil {
		fmt.Printf("PB Marshal error: %v\n", err)
	}

	_, err = nc.Request(c.NATSSubject, pb, constREQDelay)
	if err != nil {
		fmt.Printf("Request error: %v\n", err)
	}

	return nil
}

func main() {
	// SIMPLE PROGRAM TESTS THE LOGGER. Send a simple payload.

	var Cfg Config
	var err error

	config := flag.String("config", "", "Path to config file")
	flag.Parse()

	Cfg, err = getconfig(*config)

	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}

	sendLogToNATS(Cfg, nlogpb.Log{Severity: nlogpb.Severity_INFO, Message: "Test message", ServiceName: "log test server"})
}
