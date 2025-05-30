package domainerrors

import "errors"

var (
	ErrInvalidRequest = errors.New("invalid request")
	ErrInternal       = errors.New("internal error")
)
