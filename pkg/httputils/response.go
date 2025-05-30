package httputils

import (
	"ckafkago/internal/domain/domainerrors"
	"github.com/pkg/errors"
)

func BadRequest(err error, msg string) error {
	if err == nil {
		return errors.Wrap(
			domainerrors.ErrInvalidRequest,
			msg,
		)
	}

	return errors.Wrap(
		errors.Wrap(
			domainerrors.ErrInvalidRequest,
			err.Error(),
		),
		msg,
	)
}

func InternalError(err error, msg string) error {
	return errors.Wrap(
		errors.Wrap(
			domainerrors.ErrInternal,
			err.Error(),
		),
		msg,
	)
}
