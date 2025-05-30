package message

import (
	"ckafkago/internal/config"
	"context"
	"log/slog"
	"strings"

	"github.com/pkg/errors"
)

type RouterFactory struct {
	logger          *slog.Logger
	config          *config.Config
	handlerTopicMap map[string]string
}

type handler interface {
	Handle(ctx context.Context, groupID, topic string, messages []*Message) error
	GetName() string
}

func (rf RouterFactory) NewRouter() (Router, error) {
	router := NewRouter()

	err := rf.AddHandler(router, NewTestTopicHandler(rf.config, rf.logger))
	if err != nil {
		return nil, errors.Wrap(err, "failed to add register handler order handler")
	}

	return router, nil
}

func (rf RouterFactory) AddHandler(router Router, h handler) error {
	topic, ok := rf.handlerTopicMap[h.GetName()]
	if !ok {
		return errors.Errorf("topic not found for handler %s", h.GetName())
	}

	router.AddHandler(topic, h)

	return nil
}

func parseHandlerTopicMap(handlerTopicMap string) (map[string]string, error) {
	result := make(map[string]string)
	pairs := strings.Split(handlerTopicMap, ";")

	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			return nil, errors.Errorf("invalid map format, expected: handler1:topic1;handler2:topic2, actual: %s", handlerTopicMap)
		}

		topic := strings.Split(parts[0], ":")[0]
		handler := strings.Split(parts[1], ":")[0]

		result[topic] = handler
	}
	return result, nil
}

func NewRouterFactory(logger *slog.Logger, config *config.Config) (*RouterFactory, error) {
	handlerTopicMap, err := parseHandlerTopicMap(config.Kafka.Consumer.Handler.HandlerTopicMap)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse config handlerTopicMap")
	}

	return &RouterFactory{
		logger:          logger,
		config:          config,
		handlerTopicMap: handlerTopicMap,
	}, nil
}
