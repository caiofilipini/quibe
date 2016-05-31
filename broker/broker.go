package broker

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/caiofilipini/quibe/logger"
	"github.com/caiofilipini/quibe/transport"
)

type producer struct {
	clientID  string
	queueName string
	conn      net.Conn
	log       logger.Logger
}

func (p *producer) SendResponse(response string) error {
	return sendResponse(p.clientID, response, p.conn)
}

type consumer struct {
	clientID  string
	queueName string
	conn      net.Conn
	connected bool
	log       logger.Logger
}

func (c *consumer) SendResponse(response string) error {
	return sendResponse(c.clientID, response, c.conn)
}

func (c *consumer) SendMessage(m *transport.Message) error {
	err := transport.WriteMessage(c.clientID, m, c.conn)
	if err != nil {
		c.log.Warn(fmt.Sprintf("Couldn't send message to client %s: %s", c.clientID, err))
	}
	return err
}

func (c *consumer) sendError(err error) bool {
	if c.connected {
		err := c.SendResponse(transport.ErrInternal)
		return err == nil
	}
	return false
}

func (c *consumer) sendWhenReady(q *Queue) error {
	var msg *transport.Message

	// TODO: error handling here makes my eyes hurt!
	for msg == nil && c.connected {
		msg, popErr := q.Pop()
		if popErr == nil {
			sndErr := c.SendMessage(msg)

			if sndErr != nil {
				if ok := c.sendError(sndErr); !ok {
					return sndErr
				}
			}
		} else {
			err := popErr

			if err == ErrEmptyQueue {
				c.log.Debug(popErr.Error())

				err = c.SendMessage(&transport.EmptyMessage)
			}

			if err != nil {
				c.log.Error(err)

				if ok := c.sendError(err); !ok {
					return err
				}
			}
		}

		time.Sleep(1 * time.Second)
	}

	return nil
}

type queueStore struct {
	queues map[string]*Queue
	lock   sync.Mutex
	log    logger.Logger
}

func (s *queueStore) getOrCreate(name string) (*Queue, error) {
	if name == "" {
		return nil, fmt.Errorf("Invalid queue name: %s", name)
	}

	q, found := s.get(name)
	if found {
		return q, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	newQ := NewQueue(name)
	s.queues[name] = &newQ
	s.log.Info(fmt.Sprintf("Created queue %s.", name))
	return &newQ, nil
}

func (s *queueStore) get(name string) (*Queue, bool) {
	q, found := s.queues[name]
	return q, found
}

type Broker struct {
	listener net.Listener
	log      logger.Logger
	qStore   *queueStore
}

func NewBroker(host string, port int) (Broker, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Listen("tcp", addr)
	if err != nil {
		return Broker{}, err
	}

	log := logger.NewLogger("Broker")
	log.SetLevel(logger.Debug)
	log.Info(fmt.Sprintf("Listening on %s...", addr))

	return Broker{
		listener: conn,
		log:      log,
		qStore: &queueStore{
			queues: make(map[string]*Queue),
			lock:   sync.Mutex{},
			log:    logger.NewLogger("queueStore"),
		},
	}, nil
}

func (b *Broker) Start() {
	go b.handleClients()
}

func (b *Broker) handleClients() {
	for {
		clientConn, err := b.listener.Accept()
		if err != nil {
			b.log.Error(err)
			break
		}

		clientID, hsReq, err := b.handshake(clientConn)
		if err != nil {
			b.log.Error(err)
			continue
		}

		switch hsReq.ClientType {
		case transport.ClientTypeProducer:
			p := producer{
				clientID:  clientID,
				queueName: hsReq.Queue,
				conn:      clientConn,
				log:       logger.NewLogger("producer"),
			}
			go b.handleProducer(&p)
		case transport.ClientTypeConsumer:
			c := consumer{
				clientID:  clientID,
				queueName: hsReq.Queue,
				conn:      clientConn,
				connected: true,
				log:       logger.NewLogger("consumer"),
			}
			go b.handleConsumer(&c)
		}
	}
}

func (b *Broker) handleProducer(p *producer) {
	defer p.conn.Close()

	q, err := b.qStore.getOrCreate(p.queueName)
	if err != nil {
		p.SendResponse(transport.ErrInvalidQueueName)
		return
	}

	running := true
	for running {
		p.log.Info(fmt.Sprintf("Waiting for messages to queue %s...", q.Name))
		pReq, err := transport.ReadProduceRequestFrame(p.conn)

		if err != nil {
			if err == io.EOF {
				// client connection was closed...
				running = false
			}
		} else {
			p.log.Info(fmt.Sprintf("Request received: %#v", pReq))

			if pReq.ClientID != p.clientID {
				p.clientID = ""
				p.SendResponse(transport.ErrInvalidClientID)
				p.log.Info("ClientID mismatch, hanging up.")
				return
			}

			err = q.Push(pReq.Message)
			if err != nil {
				p.log.Error(fmt.Errorf("Error enqueuing message: %s", err.Error()))
			}
			p.log.Info("Message enqueued!")
			q.peek()

			p.SendResponse(transport.Success)
		}
	}

	p.log.Info("Client has closed the connection, killing worker.")
}

func (b *Broker) handleConsumer(c *consumer) {
	defer c.conn.Close()

	q, found := b.qStore.get(c.queueName)
	if !found {
		c.SendResponse(transport.ErrUnknownQueue)
		return
	}

	running := true
	for running {
		cReq, err := transport.ReadConsumeRequestFrame(c.conn)

		if err != nil {
			if err == io.EOF {
				// client connection was closed...
				running = false
				c.connected = false
			}
		} else {
			c.log.Info(fmt.Sprintf("Request received: %#v", cReq))

			if cReq.ClientID != c.clientID {
				c.clientID = ""
				c.SendResponse(transport.ErrInvalidClientID)
				c.log.Info("ClientID mismatch, hanging up.")
				return
			}

			err = c.sendWhenReady(q)
			if err != nil {
				c.log.Info("Consumer disconnected, giving up.")
				running = false
				c.connected = false
				return
			}

			q.peek()
		}
	}

	c.log.Info("Client has closed the connection, killing worker.")
}

func (b *Broker) handshake(clientConn net.Conn) (string, *transport.HandshakeRequestFrame, error) {
	hsReq, err := transport.ReadHandshakeRequest(clientConn)
	if err != nil {
		return "", nil, fmt.Errorf("Error reading handshake request frame: %v", err)
	}

	b.log.Info("Handshake request received.")

	if err = hsReq.Verify(); err != nil {
		err = transport.WriteResponse("", err.Error(), clientConn)
		if err != nil {
			b.log.Error(err)
		}

		clientConn.Close()

		return "", hsReq, fmt.Errorf("Handshake failed, terminating client connection.")
	} else {
		clientID := transport.NewUUID().String()
		return clientID, hsReq, transport.WriteResponse(clientID, transport.Success, clientConn)
	}
}

func sendResponse(clientID string, response string, w io.Writer) error {
	return transport.WriteResponse(clientID, response, w)
}
