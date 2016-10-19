package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/caiofilipini/quibe/broker"
)

func main() {
	var (
		bindAddr = flag.String("bind-addr", broker.DefaultHost, "the IP address/hostname to bind to")
		port     = flag.Int("port", broker.DefaultPort, "the port to bind to")
	)
	flag.Parse()

	b, err := broker.NewBroker(*bindAddr, *port)
	if err != nil {
		log.Fatal(err)
	}
	b.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for _ = range signals {
		log.Println("KTHXBAI.")
		os.Exit(0)
	}
}
