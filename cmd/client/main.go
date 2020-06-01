package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/BurntSushi/toml"
	nlogpb "github.com/davedotdev/proto/natslog/proto_natslog"
	"github.com/gogo/protobuf/proto"
	"github.com/nats-io/nats.go"
)

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

func main() {
	// This is the logging client.

	var Cfg Config
	var err error

	config := flag.String("config", "", "Path to config file")
	flag.Parse()

	Cfg, err = getconfig(*config)

	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}

	opts := []nats.Option{nats.Name(Cfg.NATSName)}
	opts = setupNATSConnOptions(opts)

	// Connect to NATS
	nc, err := nats.Connect(Cfg.NATSHost, opts...)
	if err != nil {
		log.Fatal(err)
	}

	defer nc.Close()

	subj := Cfg.NATSSubject

	nc.QueueSubscribe(subj, subj, func(msg *nats.Msg) {
		GPB := nlogpb.Log{}

		err := proto.Unmarshal(msg.Data, &GPB)
		if err != nil {
			fmt.Printf("Error decoding received data: %s", err)
		}

		fmt.Println(GPB.String())

		response, err := proto.Marshal(&nlogpb.Log{Code: nlogpb.Status_ACK})
		if err != nil {
			fmt.Print(err)
		}

		msg.Respond([]byte(response))

	})
	nc.Flush()

	log.Printf("LOGGING CLIENT STARTED...listening on [%s], clientID=[%s], qgroup=[%s] durable=[%s]\n", subj, Cfg.NATSName, Cfg.NATSSubject, "")

	// Wait for a SIGINT (perhaps triggered by user with CTRL-C)
	// Run cleanup when signal is received
	signalChan := make(chan os.Signal, 1)
	// cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Draining NC")
	nc.Drain()
	fmt.Println("Drained NC, exiting...")
}
