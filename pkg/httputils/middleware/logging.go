package middleware

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"

	"ckafkago/internal/app"
)

func Logging(
	h func(request *http.Request) (response *app.HTTPResponse, err error),
	group string,
) func(request *http.Request) (response *app.HTTPResponse, err error) {
	return func(request *http.Request) (response *app.HTTPResponse, err error) {
		logger := slog.Default().With(slog.String("method", request.Method), slog.String("path", request.URL.Path))
		rq, err := copyRequest(request)
		if err != nil {
			logger.Error("can't copy Request Body", slog.String("error", err.Error()))
		}
		rb, err := io.ReadAll(rq.Body)
		if err != nil {
			logger.Error("read Request Body error", slog.String("error", err.Error()))
		}
		defer func() {
			go func() {
				if err != nil {
					logger.Error(group, slog.String("request-body", string(rb)), slog.String("error", err.Error()))
					return
				}
				resp := ""
				if response.Data != nil {
					resp = string(response.Data)
				}
				switch response.Code {
				case http.StatusOK:
					logger.Info(group)
				case http.StatusInternalServerError:
					logger.Error(group, slog.String("request-body", string(rb)), slog.String("response", resp))
				case http.StatusBadRequest:
					logger.Error(group, slog.String("request-body", string(rb)), slog.String("response", resp))
				}
			}()
		}()

		return h(request)
	}
}

// Функция копирования структуры http.Request
func copyRequest(req *http.Request) (*http.Request, error) {
	// Копируем тело запроса (если оно есть)
	var bodyCopy io.ReadCloser
	if req.Body != nil {
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		// Восстанавливаем исходное тело для оригинального запроса
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		// Копируем тело для нового запроса
		bodyCopy = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	// Копируем сам запрос
	reqCopy := req.Clone(req.Context())
	reqCopy.Body = bodyCopy

	return reqCopy, nil
}
