package consumer

import (
	"ckafkago/internal/config"
	"ckafkago/pkg/kafka"
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	kafkaconsumer "ckafkago/pkg/kafka/kafka-consumer"
)

type Consumer struct {
	config             *config.Config
	logger             *slog.Logger
	kafkaHealthChecker *kafka.HealthChecker
	stopFunc           context.CancelFunc
}

func NewApp(config *config.Config) *Consumer {
	return &Consumer{
		config: config,
		logger: slog.Default(),
	}
}

func (app *Consumer) Configure() error {
	app.logger.Info("consumer app configuration started", slog.String("time", time.Now().Format("2006-01-02 15:04:05")))

	topics, err := kafkaconsumer.ExtractTopics(app.config.Kafka.Consumer.Instances)
	if err != nil {
		app.logger.Error("failed to extract topics from consumer instances", err)
		return err
	}
	app.kafkaHealthChecker = kafka.NewHealthChecker(&app.config.Kafka, topics)
	return nil
}

func (app *Consumer) Run(ctx context.Context) error {
	ctx, app.stopFunc = signal.NotifyContext(ctx, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM) // signals to graceful shutdown
	defer app.stopFunc()

	app.logger.Info("consumer app started", slog.String("time", time.Now().Format("2006-01-02 15:04:05")))

	consumerStop := make(chan struct{}, 1)
	runner := kafkaconsumer.NewRunner(app.config, app.logger)
	err := runner.Run(ctx, consumerStop)
	if err != nil {
		return err
	}

	select {
	case <-consumerStop:
		app.logger.Error("kafka.consumer.stop")
		defer app.stopFunc()
		app.logger.Info("start shutdown")
		if err != nil {
			return err
		}
		return fmt.Errorf("kafka consumer stopped")
	case <-ctx.Done():
		app.logger.Info("start shutdown", slog.String("reason", ctx.Err().Error()))
		defer app.stopFunc()
		if err != nil {
			return err
		}
	}

	return err
}
