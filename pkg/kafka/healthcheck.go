package kafka

import (
	"context"
	"strings"
	"sync"
	"time"

	"ckafkago/internal/config"
	"github.com/segmentio/kafka-go"
	"go.uber.org/multierr"
)

type HealthChecker struct {
	config  *config.Kafka
	brokers []string
	topics  []string
}

func NewHealthChecker(config *config.Kafka, topics []string) *HealthChecker {
	brokers := strings.Split(config.Brokers, ",")
	for i := range brokers {
		brokers[i] = strings.Trim(brokers[i], " ")
	}
	return &HealthChecker{
		config:  config,
		brokers: brokers,
		topics:  topics,
	}
}

func (hc *HealthChecker) Check(ctx context.Context, timeout time.Duration) error {
	errCh := make(chan error, len(hc.brokers)*len(hc.topics))
	wg := sync.WaitGroup{}
	for _, broker := range hc.brokers {
		for _, topic := range hc.topics {
			wg.Add(1)
			go func(topic, broker string) {
				defer wg.Done()
				errCh <- hc.checkBroker(ctx, broker, topic, timeout)
			}(topic, broker)
		}
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for e := range errCh {
		if e != nil {
			errs = append(errs, e)
		}
	}

	return multierr.Combine(errs...)
}

func (hc *HealthChecker) checkBroker(ctx context.Context, broker, topic string, timeout time.Duration) error {
	dialer := &kafka.Dialer{
		Timeout:  timeout,
		ClientID: "health-check",
	}

	conn, err := dialer.DialLeader(ctx, "tcp", broker, topic, 0)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.ReadPartitions()
	if err != nil {
		return err
	}

	return nil
}
