package transport

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"gopkg.in/mgo.v2/bson"
)

const (
	ProtocolName        = "quibe"
	ProtocolVersion     = "0.1"
	frameLengthByteSize = 4
)

type HandshakeRequestFrame struct {
	Protocol string
	Version  string
}

func (f *HandshakeRequestFrame) Verify() bool {
	return f.Protocol == ProtocolName && f.Version == ProtocolVersion
}

func ReadHandshakeRequest(r io.Reader) (*HandshakeRequestFrame, error) {
	log.Println("Reading handshake request frame...")
	frame, err := ReadFrame(r)
	if err != nil {
		return nil, err
	}
	hsReq := HandshakeRequestFrame{}
	err = decode(frame, &hsReq)
	return &hsReq, err
}

type HandshakeResponseFrame struct {
	Protocol string
	Version  string
	Response string
}

func WriteHandshakeResponse(success bool, w io.Writer) error {
	var response string
	if success {
		response = "OK"
	} else {
		response = "InvalidProtocol"
	}

	return WriteFrame(HandshakeResponseFrame{
		Protocol: ProtocolName,
		Version:  ProtocolVersion,
		Response: response,
	}, w)
}

// TODO accept frame type as paramenter and return that instead of []byte.
// It depends on having a common interface for frames.
func ReadFrame(r io.Reader) ([]byte, error) {
	lenBytes := make([]byte, frameLengthByteSize)
	_, err := r.Read(lenBytes)
	lenBuf := bytes.NewBuffer(lenBytes)

	var frameLen int32
	err = binary.Read(lenBuf, binary.LittleEndian, &frameLen)
	if err != nil {
		return nil, err
	}

	log.Printf("[DEBUG] Frame size: %d\n", frameLen)
	frame := make([]byte, frameLen-frameLengthByteSize)
	_, err = r.Read(frame)
	if err != nil {
		return nil, err
	}

	fullFrame := append(lenBytes, frame...)
	log.Printf("[DEBUG] Got frame: %#v\n", fullFrame)
	return fullFrame, nil
}

// TODO interface{} here sucks!
func WriteFrame(frame interface{}, w io.Writer) error {
	frameType := fmt.Sprintf("%T", frame)

	log.Printf("Sending %s...\n", frameType)
	resp, err := encode(frame)
	if err != nil {
		return err
	}

	_, err = w.Write(resp)

	if err == nil {
		log.Printf("%s sent.\n", frameType)
		log.Printf("[DEBUG] %s: %#v", frameType, resp)
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
