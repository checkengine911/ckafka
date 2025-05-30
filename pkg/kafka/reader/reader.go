package reader

import (
	"strings"
	"time"

	"ckafkago/internal/config"
	"github.com/segmentio/kafka-go"
)

func NewReader(config *config.Kafka, groupID string, topic string) *kafka.Reader {
	brokers := strings.Split(config.Brokers, ",")
	for i := range brokers {
		brokers[i] = strings.Trim(brokers[i], " ")
	}
	return kafka.NewReader(kafka.ReaderConfig{
		WatchPartitionChanges: true,
		Brokers:               brokers,
		Topic:                 topic,
		GroupID:               groupID,
		Dialer: &kafka.Dialer{
			Timeout: time.Duration(config.Timeout) * time.Second,
		},
		MaxBytes: config.Consumer.MaxBytes,
		MinBytes: config.Consumer.MinBytes,
	})
}
