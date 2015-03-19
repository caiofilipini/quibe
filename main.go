package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/caiofilipini/quibe/broker"
)

func main() {
	b, err := broker.NewBroker("localhost", 3333)
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
