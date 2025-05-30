package httputils

import (
	"fmt"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
)

type CustomValidator struct {
	valid *validator.Validate
}

func (cv *CustomValidator) Validate(i any) error {
	if err := cv.valid.Struct(i); err != nil {
		return fmt.Errorf("error while validating data | %w", err)
	}

	return nil
}

func NewValidator() (*CustomValidator, error) {
	validate := validator.New(validator.WithRequiredStructEnabled())

	err := validate.RegisterValidation("client", ClientValidation)
	if err != nil {
		return nil, errors.Wrap(err, "register client validator")
	}

	return &CustomValidator{valid: validate}, nil
}

func ClientValidation(fl validator.FieldLevel) bool {
	maxFields := fl.Field().Type().NumField()
	isValid := false
	for i := 0; i < maxFields; i++ {
		fv := fl.Field().Field(i)

		switch fv.Kind() {
		case reflect.String:
			if fv.String() != "" {
				isValid = true
			}
		case reflect.Int:
			if fv.Int() != 0 {
				isValid = true
			}
		}
	}

	return isValid
}
