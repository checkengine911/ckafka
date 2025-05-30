package main

import (
	"ckafkago/internal/app"
	"ckafkago/internal/bootstrap"
	"ckafkago/internal/config"
	"ckafkago/internal/consumer"
	"ckafkago/internal/handler"
	"log"
	"log/slog"
	"os"
)

func fatalIfErr(err error, msg string) {
	log.Fatal(err, msg)
	os.Exit(1)
}

func main() {
	cfg, err := config.CreateConfig()
	if err != nil {
		fatalIfErr(err, "CreateConfig")
	}

	bootstrap.InitLogger(cfg.Logger)
	serverApp := app.New(cfg.AppConfig, slog.Default())

	ctx := serverApp.GetShutdownContext()

	consumerApp := consumer.NewApp(cfg)

	if err := consumerApp.Configure(); err != nil {
		fatalIfErr(err, "Configure")
	}

	handler.InitRouter(serverApp)

	go serverApp.Run()
	if err := consumerApp.Run(ctx); err != nil {
		fatalIfErr(err, "Run")
	}
}
