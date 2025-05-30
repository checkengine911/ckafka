package bootstrap

import (
	"log/slog"
	"os"
	"strings"
	"sync"

	"ckafkago/internal/config"
)

var loggerOnce = sync.Once{}

func InitLogger(cfg config.Logger) {
	loggerOnce.Do(func() {
		logger := slog.New(
			slog.NewJSONHandler(os.Stdout,
				&slog.HandlerOptions{
					Level: func() slog.Leveler {
						switch strings.ToLower(cfg.Level) {
						case "debug":
							return slog.LevelDebug
						case "info":
							return slog.LevelInfo
						case "warn":
							return slog.LevelWarn
						case "error":
							return slog.LevelError
						default:
							return slog.LevelInfo
						}
					}(),
				},
			),
		)
		slog.SetDefault(logger)
	})
}
