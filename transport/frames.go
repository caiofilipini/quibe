package transport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/caiofilipini/quibe/logger"
	"gopkg.in/mgo.v2/bson"
)

const (
	ProtocolName     = "quibe"
	ProtocolVersion  = "0.1"
	frameLenByteSize = 4

	ClientTypeProducer = "P"
	ClientTypeConsumer = "C"

	Success              = "OK"
	ErrUnknownProtocol   = "UnknownProtocol"
	ErrInvalidVersion    = "InvalidVersion"
	ErrUnsupportedClient = "UnsupportedClient"
	ErrInvalidClientID   = "InvalidClientID"
	ErrInvalidQueueName  = "InvalidQueueName"
	ErrUnknownQueue      = "UnknownQueue"
	ErrInternal          = "InternalError"
)

var (
	log logger.Logger

	responseCodes = map[string]uint32{
		Success:              0x0,
		ErrUnknownProtocol:   0x1,
		ErrInvalidVersion:    0x2,
		ErrUnsupportedClient: 0x3,
		ErrInvalidClientID:   0x4,
		ErrInvalidQueueName:  0x5,
		ErrUnknownQueue:      0x6,
		ErrInternal:          0x63,
	}
)

func init() {
	log = logger.NewLogger("transport")
	log.SetLevel(logger.Debug)
}

type HandshakeRequestFrame struct {
	Protocol   string
	Version    string
	ClientType string
	Queue      string
}

func (f *HandshakeRequestFrame) Verify() error {
	if f.Protocol != ProtocolName {
		return fmt.Errorf(ErrUnknownProtocol)
	}
	if f.Version != ProtocolVersion {
		return fmt.Errorf(ErrInvalidVersion)
	}
	if f.ClientType != ClientTypeProducer && f.ClientType != ClientTypeConsumer {
		return fmt.Errorf(ErrUnsupportedClient)
	}
	return nil
}

func ReadHandshakeRequest(r io.Reader) (*HandshakeRequestFrame, error) {
	log.Info("Reading handshake request frame...")
	frame, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	hsReq := HandshakeRequestFrame{}
	err = decode(frame, &hsReq)
	return &hsReq, err
}

type ResponseFrame struct {
	Protocol     string
	Version      string
	ClientID     string
	Success      bool
	Response     string
	ResponseCode uint32
}

func WriteResponse(clientID string, response string, w io.Writer) error {
	return WriteFrame(ResponseFrame{
		Protocol:     ProtocolName,
		Version:      ProtocolVersion,
		ClientID:     clientID,
		Success:      response == Success,
		Response:     response,
		ResponseCode: responseCodes[response],
	}, w)
}

type ProduceRequestFrame struct {
	ClientID string
	Queue    string
	Message  Message
}

type MessageFrame struct {
	ClientID string
	Message  Message
}

func WriteMessage(clientID string, msg *Message, w io.Writer) error {
	return WriteFrame(MessageFrame{
		ClientID: clientID,
		Message:  *msg,
	}, w)
}

func ReadProduceRequestFrame(r io.Reader) (*ProduceRequestFrame, error) {
	log.Info("Reading produce request frame...")
	frame, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(frame))
	pReq := ProduceRequestFrame{}
	err = decode(frame, &pReq)
	if err != nil {
		panic(err)
	}
	return &pReq, err
}

type ConsumeRequestFrame struct {
	ClientID string
	Queue    string
}

func ReadConsumeRequestFrame(r io.Reader) (*ConsumeRequestFrame, error) {
	log.Info("Reading consume request frame...")
	frame, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(frame))
	cReq := ConsumeRequestFrame{}
	err = decode(frame, &cReq)
	if err != nil {
		panic(err)
	}
	return &cReq, err
}

// TODO accept frame type as parameter and return that instead of []byte.
// It depends on having a common interface for frames.
func ReadFrame(r io.Reader) ([]byte, error) {
	lenBytes := make([]byte, frameLenByteSize)
	_, err := r.Read(lenBytes)
	if err != nil {
		return nil, err
	}
	lenBuf := bytes.NewBuffer(lenBytes)

	var frameLen int32
	err = binary.Read(lenBuf, binary.LittleEndian, &frameLen)
	if err != nil {
		return nil, err
	}

	log.Debug(fmt.Sprintf("Frame size: %d", frameLen))
	frame := make([]byte, frameLen-frameLenByteSize)
	_, err = r.Read(frame)
	if err != nil {
		return nil, err
	}

	fullFrame := append(lenBytes, frame...)
	log.Debug(fmt.Sprintf("Got frame: %#v", fullFrame))
	return fullFrame, nil
}

// TODO interface{} here sucks!
func WriteFrame(frame interface{}, w io.Writer) error {
	frameType := fmt.Sprintf("%T", frame)

	log.Info(fmt.Sprintf("Sending %s...", frameType))
	resp, err := encode(frame)
	if err != nil {
		return err
	}

	_, err = w.Write(resp)

	if err == nil {
		log.Info(fmt.Sprintf("%s sent.", frameType))
		log.Debug(fmt.Sprintf("%s: %#v", frameType, resp))
	}

	return err
}

func decode(in []byte, out interface{}) error {
	return bson.Unmarshal(in, out)
}

func encode(in interface{}) ([]byte, error) {
	out, err := bson.Marshal(in)
	if err != nil {
		return nil, err
	}
	return out, nil
}
