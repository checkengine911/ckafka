package message

import (
	"ckafkago/internal/config"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"go.uber.org/multierr"
)

const (
	TestTopicHandlerName = "test-topic-handler"
)

type TestTopicHandler struct {
	logger *slog.Logger
	config *config.Config
}

func isInitMsg(msg []byte) bool {
	var m map[string]string
	jsoniter.Unmarshal(msg, &m)
	if v, ok := m["message"]; ok && strings.Contains(v, "initialized") {
		return true
	}
	return false
}

func (h *TestTopicHandler) Handle(ctx context.Context, groupID, topic string, messages []*Message) (err error) {
	errs := make([]error, 0)
	for _, msg := range messages {
		if isInitMsg(msg.Value) {
			continue
		}

		var decodedMsg Msg
		err = json.Unmarshal(msg.Value, &decodedMsg)
		if err != nil {
			errs = append(errs, err)
		}
		h.logger.Info(fmt.Sprintf("Decoded Message:ID: %s Content: %s", decodedMsg.MessageID, decodedMsg.Message))

	}

	if len(errs) == 0 {
		h.logger.Info("successfully fetch messages", slog.String("topic", topic), slog.String("groupID", groupID))
	} else {
		h.logger.Warn("fetch and send statuses batch with errors", slog.String("topic", topic), slog.String("groupID", groupID))
	}

	return multierr.Combine(errs...)
}

func (h *TestTopicHandler) GetName() string {
	return TestTopicHandlerName
}

func NewTestTopicHandler(config *config.Config, logger *slog.Logger) *TestTopicHandler {
	return &TestTopicHandler{
		logger: logger,
		config: config,
	}
}
