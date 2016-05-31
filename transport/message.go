package transport

type Header struct{}

type Message struct {
	Headers map[string]Header
	Body    []byte
}

const (
	emptyBody = "\\0"
)

var (
	EmptyMessage = NewMessage([]byte(emptyBody))
)

func NewMessage(body []byte) Message {
	return Message{
		Headers: make(map[string]Header),
		Body:    body,
	}
}

func (m *Message) BodyString() string {
	return string(m.Body)
}

func (m *Message) IsEmpty() bool {
	return string(m.Body) == emptyBody
}
