package config

import (
	"os"
	"strings"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/pkg/errors"
)

type Config struct {
	Env       string    `json:"env" env-default:"local"`
	AppConfig AppConfig `json:"app_config"`
	Logger    Logger    `json:"logger"`
	Kafka     Kafka     `json:"kafka"`
}

type AppConfig struct {
	HTTPServer                    HTTPServer `json:"http_server"`
	GracefulShutdownTimeoutSecond int64      `json:"graceful_shutdown_timeout_second"`
}

type HTTPServer struct {
	Port              int `json:"port"`
	ReadTimeoutSecond int `json:"read_timeout_second"`
}

type Logger struct {
	Level string `json:"level" env:"LOGGER_LEVEL"`
}

type Kafka struct {
	Brokers  string `json:"brokers" env:"KAFKA_BROKERS"`
	Login    string `json:"login" env:"KAFKA_LOGIN"`
	Password string `json:"password" env:"KAFKA_PASSWORD"`
	Timeout  int    `json:"timeout" env:"KAFKA_TIMEOUT" env-default:"5"`

	TLSCert []byte `json:"TLSCert" env:"KAFKA_TLS_CERT"`

	Consumer KafkaConsumer `json:"consumer"`
}
type KafkaConsumer struct {
	MaxBytes         int                   `json:"maxBytes" env:"KAFKA_CONSUMER_MAX_BYTES" env-default:"1048576"`             // indicates to the broker the maximum batch size that the reader will accept
	MinBytes         int                   `json:"minBytes" env:"KAFKA_CONSUMER_MIN_BYTES" env-default:"1"`                   // indicates to the broker the minimum batch size that the reader will accept
	Instances        string                `json:"instances" env:"KAFKA_CONSUMER_INSTANCES"`                                  // semicolon separated list of item with format: "groupID:{groupID},topic:{topic},count:{count}"
	ReadBatchSize    int                   `json:"readBatchSize" env:"KAFKA_CONSUMER_READ_BATCH_SIZE" env-default:"5" `       // number of messages to read in one batch
	FlushInterval    int                   `json:"flushInterval" env:"KAFKA_CONSUMER_FLUSH_INTERVAL" env-default:"10000"`     // flush interval in milliseconds
	FlushMaxRetries  int                   `json:"flushMaxRetries" env:"KAFKA_CONSUMER_FLUSH_MAX_RETRIES" env-default:"10"`   // number of retries to flush messages
	FetchMaxRetries  int                   `json:"fetchMaxRetries" env:"KAFKA_CONSUMER_FETCH_MAX_RETRIES" env-default:"10"`   // number of retries to fetch messages
	CommitMaxRetries int                   `json:"commitMaxRetries" env:"KAFKA_CONSUMER_COMMIT_MAX_RETRIES" env-default:"20"` // number of retries to commit messages
	Handler          ConsumerHandlerConfig `json:"handler"`
}
type ConsumerHandlerConfig struct {
	HandlerTopicMap string `json:"handlerTopicMap" env:"KAFKA_CONSUMER_HANDLER_TOPIC_MAP"` // semicolon separated list of item with format: "{handler}:{topic}"
}

func CreateConfig() (*Config, error) {
	configPath := os.Getenv("CONFIG_PATH")
	if strings.TrimSpace(configPath) == "" {
		return nil, errors.New("CONFIG_PATH is not set")
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, errors.Errorf("config file does not exist - `%s`", configPath)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		return nil, errors.Wrap(err, "cannot read config")
	}

	return &cfg, nil
}
