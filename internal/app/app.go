package app

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"ckafkago/internal/app/closer"
	"ckafkago/internal/config"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/pkg/errors"
)

type App struct {
	config config.AppConfig

	publicMiddlewares []func(http.Handler) http.Handler

	shutdownCtx  context.Context
	publicRouter chi.Router

	closer *closer.Closer
	logger *slog.Logger
}

func New(config config.AppConfig, logger *slog.Logger) *App {
	ctx := context.Background()
	shutdownCtx, cancel := context.WithCancel(ctx)

	app := &App{
		config:      config,
		logger:      logger,
		shutdownCtx: shutdownCtx,
		closer:      closer.New(logger),
	}

	app.closer.Add(func() error {
		cancel()
		return nil
	})
	app.publicMiddlewares = append(app.publicMiddlewares, middleware.Recoverer)

	return app
}

func (x *App) GetShutdownContext() context.Context {
	return x.shutdownCtx
}

func (x *App) RegisterHTTPHandler(method HTTPMethod, pattern string, handler HandlerFn) {
	if x.publicRouter == nil {
		x.initHTTPRouter()
	}

	innerHandler := func(writer http.ResponseWriter, request *http.Request) {
		response, err := handler(request)

		if response != nil {
			for _, addEntry := range response.Headers.GetAddEntrySlice() {
				writer.Header().Add(addEntry.Name, addEntry.Value)
			}

			for name, value := range response.Headers.GetSetEntryMap() {
				writer.Header().Set(name, value)
			}
		}

		if err != nil {
			var resultCode int

			if response != nil && response.Code != 0 {
				resultCode = response.Code
			} else {
				resultCode = http.StatusInternalServerError
			}

			setErrorResponse(err, resultCode, writer, request)
			return
		}

		writer.WriteHeader(http.StatusOK)

		_, err = writer.Write(response.Data)
		if err != nil {
			setErrorResponse(err, http.StatusInternalServerError, writer, request)
			return
		}
	}

	switch method {
	case Get:
		x.publicRouter.Get(pattern, innerHandler)
	case Post:
		x.publicRouter.Post(pattern, innerHandler)
	case Head:
		x.publicRouter.Head(pattern, innerHandler)
	case Put:
		x.publicRouter.Put(pattern, innerHandler)
	case Patch:
		x.publicRouter.Patch(pattern, innerHandler)
	case Delete:
		x.publicRouter.Delete(pattern, innerHandler)
	case Options:
		x.publicRouter.Options(pattern, innerHandler)
	default:
	}
}

func (x *App) Run() {
	notifyCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	x.runPublicHTTPServer()

	<-notifyCtx.Done()

	x.closer.CloseAll()
}

func (x *App) WithPublicMiddlewares(publicMiddlewares ...func(http.Handler) http.Handler) {
	x.publicMiddlewares = append(x.publicMiddlewares, publicMiddlewares...)
}

func (x *App) runPublicHTTPServer() {
	if x.publicRouter == nil {
		x.initHTTPRouter()
	}

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", x.publicHTTPPort()),
		Handler:           x.publicRouter,
		ReadHeaderTimeout: time.Duration(x.config.HTTPServer.ReadTimeoutSecond) * time.Second,
	}

	x.logger.Info(fmt.Sprintf("http: starting public server on port %d", x.publicHTTPPort()))

	go func() {
		serverErr := httpServer.ListenAndServe()
		if serverErr != nil && errors.Is(serverErr, http.ErrServerClosed) {
			return
		}

		log.Fatal(serverErr, "http: cat`t start public server")
	}()

	x.closer.Add(
		func() error {
			x.logger.Info("http: stopping public server")

			GracefulShutdownTimeoutSecond := time.Duration(x.config.GracefulShutdownTimeoutSecond)
			if GracefulShutdownTimeoutSecond == 0 {
				GracefulShutdownTimeoutSecond = 5 * time.Second
			}
			GracefulShutdownTimeoutSecond *= time.Second

			ctx, cancel := context.WithTimeout(context.Background(), GracefulShutdownTimeoutSecond)
			defer cancel()

			if err := httpServer.Shutdown(ctx); err != nil {
				return errors.Wrap(err, "http: failed to stop public server")
			}

			return nil
		},
	)
}

func (x *App) initHTTPRouter() {
	router := chi.NewRouter()

	for _, publicMiddleware := range x.publicMiddlewares {
		router.Use(publicMiddleware)
	}

	x.publicRouter = router
}

func (x *App) publicHTTPPort() int {
	if x.config.HTTPServer.Port == 0 {
		return 8080
	}

	return x.config.HTTPServer.Port
}
