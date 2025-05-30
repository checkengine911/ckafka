package middleware

import (
	"net/http"

	"ckafkago/internal/app"
	"ckafkago/internal/domain/domainerrors"

	"github.com/pkg/errors"
)

func Finalizer(
	h func(request *http.Request) (response *app.HTTPResponse, err error),
	group string,
) func(request *http.Request) (response *app.HTTPResponse, err error) {
	return func(request *http.Request) (response *app.HTTPResponse, err error) {
		response, err = h(request)

		if response == nil {
			response = &app.HTTPResponse{}
		}

		response.Headers.Set("Content-Type", "application/json; charset=utf-8")
		response.Headers.Set("Cache-Control", "no-cache, no-store, must-revalidate")
		response.Headers.Set("Pragma", "no-cache")
		response.Headers.Set("Expires", "0")

		if err != nil {
			response.Headers.Set("X-Error", err.Error())
			switch {
			case errors.Is(err, domainerrors.ErrInvalidRequest):
				response.Code = http.StatusBadRequest
			default:
				response.Code = http.StatusInternalServerError
			}
		} else {
			response.Code = http.StatusOK
		}

		return response, err
	}
}
