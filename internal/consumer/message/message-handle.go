package message

import (
	"context"
	"log/slog"
)

type HandleMessagesCommand struct {
	groupID string
	topic   string
	logger  *slog.Logger
	router  Router
}

type HandleMessagesCommandOptions struct {
	Messages []*Message
}

func (c *HandleMessagesCommand) Execute(ctx context.Context, options HandleMessagesCommandOptions) error {
	return c.router.Handle(ctx, c.groupID, c.topic, options.Messages)
}

func NewHandleMessageCommand(groupID string, topic string, router Router) Command[HandleMessagesCommandOptions] {
	return &HandleMessagesCommand{
		groupID: groupID,
		topic:   topic,
		logger:  slog.Default(),
		router:  router,
	}
}

type Command[O any] interface {
	Execute(ctx context.Context, options O) error
}
