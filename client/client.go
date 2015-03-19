package main

import (
	"log"
	"net"

	"github.com/caiofilipini/quibe/transport"
	"gopkg.in/mgo.v2/bson"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:3333")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	log.Println("Connected to broker.")

	hsReq := transport.HandshakeRequestFrame{
		Protocol: transport.ProtocolName,
		Version:  transport.ProtocolVersion,
	}

	bts, err := bson.Marshal(&hsReq)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[DEBUG] Encoded %T: %#v\n", hsReq, bts)
	log.Printf("Sending %T...\n", hsReq)
	_, err = conn.Write(bts)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%T sent.\n", hsReq)

	log.Println("Reading handshake response frame...")
	frame, err := transport.ReadFrame(conn)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[DEBUG] Handshake response frame: %#v\n", frame)
	log.Printf("[DEBUG] Handshake response frame: %s\n", string(frame))
}
