package transport

type Header struct{}

type Message struct {
	Headers map[string]Header
	Body    []byte
}

func NewMessage(body []byte) Message {
	return Message{
		Headers: make(map[string]Header),
		Body:    body,
	}
}

func (m *Message) BodyString() string {
	return string(m.Body)
}
