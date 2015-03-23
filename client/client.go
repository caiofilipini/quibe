package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/caiofilipini/quibe/transport"
	"gopkg.in/mgo.v2/bson"
)

func produce() {
	pconn, err := net.Dial("tcp", "localhost:3333")
	if err != nil {
		log.Fatal(err)
	}
	defer pconn.Close()
	log.Println("[Producer] Connected to broker.")

	hsReq := transport.HandshakeRequestFrame{
		Protocol:   transport.ProtocolName,
		Version:    transport.ProtocolVersion,
		ClientType: transport.ClientTypeProducer,
		Queue:      "foo",
	}
	send(hsReq, pconn)

	rf := readResponse("Producer", pconn)

	var clientID string
	if rf.Response == transport.Success {
		clientID = rf.ClientID
		id := 1

		ticker := time.NewTicker(1 * time.Second)
		for _ = range ticker.C {
			msg := transport.NewMessage([]byte(fmt.Sprintf("message %d", id)))
			pr := transport.ProduceRequestFrame{
				ClientID: clientID,
				Message:  msg,
				Queue:    "foo",
			}
			send(pr, pconn)

			rf = readResponse("Producer", pconn)

			if rf.Response != transport.Success {
				break
			}

			id++
		}
	}

	log.Printf("[Producer] Done.")
}

func consume() {
	cconn, err := net.Dial("tcp", "localhost:3333")
	if err != nil {
		log.Fatal(err)
	}
	defer cconn.Close()
	log.Println("[Consumer] Connected to broker.")

	hsReq := transport.HandshakeRequestFrame{
		Protocol:   transport.ProtocolName,
		Version:    transport.ProtocolVersion,
		ClientType: transport.ClientTypeConsumer,
		Queue:      "foo",
	}
	send(hsReq, cconn)

	rf := readResponse("Consumer", cconn)

	if rf.Response == transport.Success {
		ticker := time.NewTicker(300 * time.Millisecond)

		for _ = range ticker.C {
			crf := transport.ConsumeRequestFrame{
				ClientID: rf.ClientID,
				Queue:    "foo",
			}
			send(crf, cconn)

			frame, err := transport.ReadFrame(cconn)
			dieOn(err)

			log.Printf("[Consumer] [DEBUG] Message frame: %#v\n", frame)
			log.Printf("[Consumer] [DEBUG] Message frame: %s\n", string(frame))

			mf := transport.MessageFrame{}
			err = bson.Unmarshal(frame, &mf)
			dieOn(err)
			log.Printf("[Consumer] Consuming %s\n", mf.Message.BodyString())
		}
	}

	log.Printf("[Consumer] Done.")
}

func main() {
	go produce()
	go consume()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for _ = range signals {
		log.Println("KTHXBAI.")
		os.Exit(0)
	}
}

func send(frame interface{}, w io.Writer) {
	bts, err := bson.Marshal(frame)
	dieOn(err)

	log.Printf("[DEBUG] Encoded %T: %#v\n", frame, bts)
	log.Printf("Sending %T...\n", frame)
	_, err = w.Write(bts)
	dieOn(err)
	log.Printf("%T sent.\n", frame)
}

func dieOn(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func readResponse(who string, conn net.Conn) transport.ResponseFrame {
	log.Printf("[%s] Reading response frame...\n", who)
	frame, err := transport.ReadFrame(conn)
	dieOn(err)

	log.Printf("[%s] [DEBUG] Response frame: %#v\n", who, frame)
	log.Printf("[%s] [DEBUG] Response frame: %s\n", who, string(frame))

	rf := transport.ResponseFrame{}
	err = bson.Unmarshal(frame, &rf)
	dieOn(err)
	log.Printf("[%s] [DEBUG] Response code: %d\n", who, rf.ResponseCode)
	log.Printf("[%s] [DEBUG] Response message: %s\n", who, rf.Response)

	return rf
}
