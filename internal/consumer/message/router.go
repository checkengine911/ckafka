package message

import (
	"context"

	"github.com/pkg/errors"
)

type Handler interface {
	Handle(ctx context.Context, groupID, topic string, messages []*Message) error
}

type Router map[string]Handler

func (r Router) AddHandler(topic string, handler Handler) Router {
	r[topic] = handler

	return r
}

func (r Router) Handle(ctx context.Context, groupID, topic string, messages []*Message) error {
	if h, ok := r[topic]; ok {
		if err := h.Handle(ctx, groupID, topic, messages); err != nil {
			return errors.Wrap(err, "failed to handle messages")
		}
	}

	return nil
}

func NewRouter() Router {
	return make(Router)
}
