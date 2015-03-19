package broker

import (
	"fmt"
	"log"
	"net"

	"github.com/caiofilipini/quibe/transport"
)

type Producer struct {
	conn net.Conn
}

type Consumer struct {
	conn net.Conn
}

type Subscription struct {
	consumer Consumer
	queue    *Queue
}

type Broker struct {
	conn          net.Listener
	queues        map[string]*Queue
	producers     map[string]*Producer
	subscriptions map[string]*Subscription
}

func NewBroker(host string, port int) (Broker, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Listen("tcp", addr)
	if err != nil {
		return Broker{}, err
	}
	log.Printf("Listening on %s...\n", addr)

	return Broker{
		conn:          conn,
		queues:        make(map[string]*Queue),
		producers:     make(map[string]*Producer),
		subscriptions: make(map[string]*Subscription),
	}, nil
}

func (b *Broker) Start() error {
	go b.handleClients()
	return nil
}

func (b *Broker) handleClients() {
	for {
		clientConn, err := b.conn.Accept()
		if err != nil {
			log.Println(err)
			break
		}

		err = b.handshake(clientConn)
		if err != nil {
			fmt.Printf("Error performing handshake: %v\n", err)
			break
		}

		// TODO identify producer/consumer and register them accordingly
	}
}

func (b *Broker) handshake(clientConn net.Conn) error {
	hsReq, err := transport.ReadHandshakeRequest(clientConn)
	if err != nil {
		return fmt.Errorf("Error reading handshake request frame: %v", err)
	}

	log.Printf("Handshake request received.")
	return transport.WriteHandshakeResponse(hsReq.Verify(), clientConn)
}
