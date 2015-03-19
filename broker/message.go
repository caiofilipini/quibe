package broker

type Header struct{}

type Message struct {
	headers map[string]Header
	body    []byte
}

func NewMessage(body []byte) (Message, error) {
	return Message{
		headers: make(map[string]Header),
		body:    body,
	}, nil
}

func (m *Message) Body() []byte {
	return m.body
}

func (m *Message) BodyString() string {
	return string(m.body)
}
