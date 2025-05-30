package kafka_consumer

import (
	"ckafkago/internal/config"
	"ckafkago/internal/consumer/message"
	"ckafkago/pkg/kafka/reader"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

type Runner struct {
	config *config.Config
	logger *slog.Logger
}

func (r *Runner) Run(ctx context.Context, stop chan<- struct{}) error {
	instances, err := parseInstances(r.config.Kafka.Consumer.Instances)
	if err != nil {
		r.logger.Error("failed to parse consumer instances", err.Error())
		return fmt.Errorf("failed to parse consumer instances")
	}

	routerFactory, err := message.NewRouterFactory(r.logger, r.config)
	if err != nil {
		r.logger.Error("failed to create router factory", err)
		return fmt.Errorf("failed to create router factory")
	}

	router, err := routerFactory.NewRouter()
	if err != nil {
		r.logger.Error("failed to create router", err)
		return fmt.Errorf("failed to create router")
	}

	for i, instance := range instances {
		for k := 0; k < instance.Count; k++ {
			consumerID := fmt.Sprintf("%d-%d", i, k)
			r.logger.Info(fmt.Sprintf("starting consumer #%s", consumerID), slog.String("topic", instance.Topic), slog.String("groupID", instance.GroupID), slog.String("consumerID", consumerID))
			kafkaConsumer := NewConsumer(
				reader.NewReader(&r.config.Kafka, instance.GroupID, instance.Topic),
				message.NewHandleMessageCommand(instance.GroupID, instance.Topic, router),
				&r.config.Kafka.Consumer, consumerID,
			)

			middleStopCh := make(chan struct{})

			go kafkaConsumer.Run(ctx, middleStopCh)

			go func() {
				select {
				case <-middleStopCh:
					stop <- struct{}{}
					return
				}
			}()
		}
	}

	return nil
}

type InstanceInfo struct {
	GroupID string
	Topic   string
	Count   int
}

func parseInstances(instances string) ([]InstanceInfo, error) {
	var result []InstanceInfo
	pairs := strings.Split(instances, ";")

	for _, pair := range pairs {
		parts := strings.Split(pair, ",")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid instance format")
		}

		groupID := strings.Split(parts[0], ":")[1]
		topic := strings.Split(parts[1], ":")[1]
		count, err := strconv.Atoi(strings.Split(parts[2], ":")[1])
		if err != nil {
			return nil, err
		}

		result = append(result, InstanceInfo{
			GroupID: groupID,
			Topic:   topic,
			Count:   count,
		})
	}

	return result, nil
}

func ExtractTopics(instances string) ([]string, error) {
	infoInstances, err := parseInstances(instances)
	if err != nil {
		return nil, fmt.Errorf("failed to parse consumer instances: %w", err)
	}
	var topics []string
	for _, instance := range infoInstances {
		topics = append(topics, instance.Topic)
	}
	return topics, nil
}

func NewRunner(config *config.Config, logger *slog.Logger) *Runner {
	return &Runner{
		config: config,
		logger: logger,
	}
}
