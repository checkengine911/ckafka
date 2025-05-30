package kafka_consumer

import (
	"ckafkago/internal/config"
	"ckafkago/internal/consumer/message"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader                *kafka.Reader
	logger                *slog.Logger
	handleMessagesCommand message.Command[message.HandleMessagesCommandOptions]
	config                *config.KafkaConsumer
	consumerID            string
}

func NewConsumer(
	reader *kafka.Reader,
	handleMessagesCommand message.Command[message.HandleMessagesCommandOptions],
	config *config.KafkaConsumer,
	consumerID string,
) *Consumer {
	return &Consumer{
		reader:                reader,
		logger:                slog.Default(),
		handleMessagesCommand: handleMessagesCommand,
		config:                config,
		consumerID:            consumerID,
	}
}

func (consumer *Consumer) Run(ctx context.Context, stop chan<- struct{}) {
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("%v", r)
			}

			stack := string(debug.Stack())

			consumer.logger.Error("panic in run",
				"err", err,
				slog.String("consumerID", consumer.consumerID),
				slog.String("stack", stack))
			stop <- struct{}{}
		}
	}()

	batchSize := consumer.config.ReadBatchSize
	messages := make(chan *kafka.Message, batchSize)
	defer close(messages)

	go consumer.handleMessages(ctx, messages, stop)

	for {
		select {
		case <-ctx.Done():
			if err := consumer.reader.Close(); err != nil {
				consumer.logger.Error(fmt.Sprintf("close consumer err: %s", err.Error()))
			}
			return
		default:
		}

		msg, err := consumer.tryToFetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				consumer.logger.Info("stop fetch messages", slog.String("consumerID", consumer.consumerID))
			} else {
				consumer.logger.Error(fmt.Sprintf("no longer possible to fetch messages due to error: %s", err.Error()), slog.String("consumerID", consumer.consumerID))
			}
			stop <- struct{}{}
			return
		}

		messages <- msg
	}
}

func (consumer *Consumer) handleMessages(ctx context.Context, messages <-chan *kafka.Message, stop chan<- struct{}) {
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("%v", r)
			}

			stack := string(debug.Stack())

			consumer.logger.Error("panic in handle messages",
				"err", err,
				slog.String("consumerID", consumer.consumerID),
				slog.String("stack", stack))
			stop <- struct{}{}
		}
	}()

	batchSize := consumer.config.ReadBatchSize
	flushInterval := time.Duration(consumer.config.FlushInterval) * time.Millisecond

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var msg *kafka.Message
			batch := make([]*message.Message, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				var ok bool
				select {
				case msg, ok = <-messages:
					if !ok {
						return
					} else if msg != nil {
						batch = append(batch, transformMessage(msg))
					}
				default:
					break
				}
			}

			if len(batch) > 0 {
				err := consumer.tryToRun(func(retry int) error {
					err := consumer.handleMessagesCommand.Execute(ctx, message.HandleMessagesCommandOptions{Messages: batch})
					if err != nil {
						var processError *ProcessError
						if errors.As(err, &processError) {
							return processError
						}
						err = fmt.Errorf("handle message error: %w", err)
						consumer.logger.Error(err.Error(), slog.String("consumerID", consumer.consumerID))
					}
					return nil
				}, consumer.config.FlushMaxRetries)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						consumer.logger.Info("stop handle messages", slog.String("consumerID", consumer.consumerID))
					} else {
						err = fmt.Errorf("no longer possible to handle messages due to error: %w", err)
						consumer.logger.Error(err.Error(), slog.String("consumerID", consumer.consumerID))
					}
					stop <- struct{}{}
					return
				}
			}

			if msg != nil {
				err := consumer.tryToCommitMessages(ctx, *msg)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						consumer.logger.Info("stop commit messages", slog.String("consumerID", consumer.consumerID))
					} else {
						err = fmt.Errorf("no longer possible to commit messages due to error: %w", err)
						consumer.logger.Error(err.Error(), slog.String("consumerID", consumer.consumerID))
					}
					stop <- struct{}{}
					return
				}
			}

			if len(batch) > 0 {
				ticker.Reset(flushInterval)
			}
		}
	}
}

func (consumer *Consumer) tryToCommitMessages(ctx context.Context, message kafka.Message) error {
	err := consumer.tryToRun(func(retry int) error {
		return consumer.reader.CommitMessages(ctx, message)
	}, consumer.config.CommitMaxRetries)
	if err != nil {
		return fmt.Errorf("try to commit message error: %w", err)
	}

	return nil
}

func (consumer *Consumer) tryToFetchMessage(ctx context.Context) (*kafka.Message, error) {
	var m kafka.Message
	err := consumer.tryToRun(func(retry int) error {
		var err error
		m, err = consumer.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		return nil
	}, consumer.config.FetchMaxRetries)
	if err != nil {
		return nil, fmt.Errorf("try to fetch message error: %w", err)
	}

	return &m, nil
}

func (consumer *Consumer) tryToRun(run func(retry int) error, maxTries int) error {
	var err error
	for i := 0; i < maxTries; i++ {
		err = run(i + 1)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			consumer.logger.Error(fmt.Sprintf("try to run error: %s", err.Error()), slog.Int("try", i+1))
			waitFor := int64(math.Pow(float64(i+1), float64(2)))
			time.Sleep(time.Duration(waitFor) * time.Second)
			continue
		}
		return nil
	}

	return fmt.Errorf("failed after %d tries: %w", maxTries, err)
}

func transformMessage(msg *kafka.Message) *message.Message {
	if msg == nil {
		panic("message is nil")
	}
	return &message.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Time:      msg.Time,
	}
}

func transformMessageHeaders(headers []kafka.Header) message.Headers {
	result := make(message.Headers, len(headers))
	for _, header := range headers {
		result[header.Key] = header.Value
	}

	return result
}
