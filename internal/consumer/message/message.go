package message

import "time"

type Msg struct {
	MessageID string `json:"messageId"`
	Message   string `json:"message"`
}

type Message struct {
	Headers       Headers
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Time          time.Time
}

type Headers map[string][]byte

func (h Headers) Has(key string) bool {
	_, ok := h[key]
	return ok
}
