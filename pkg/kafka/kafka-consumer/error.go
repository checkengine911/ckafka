package kafka_consumer

type ProcessError struct {
	Err error
}

func (e *ProcessError) Error() string {
	return e.Err.Error()
}

func (e *ProcessError) Unwrap() error {
	return e.Err
}

func NewProcessError(err error) *ProcessError {
	return &ProcessError{Err: err}
}
