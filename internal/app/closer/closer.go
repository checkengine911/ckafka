package closer

import (
	"log/slog"
	"sync"

	"github.com/pkg/errors"
)

type Closer struct {
	mutex        sync.Mutex
	once         sync.Once
	closersFuncs []func() error
	logger       *slog.Logger
}

func New(logger *slog.Logger) *Closer {
	return &Closer{
		logger: logger,
	}
}

func (x *Closer) Add(closer func() error) {
	x.mutex.Lock()
	x.closersFuncs = append(x.closersFuncs, closer)
	x.mutex.Unlock()
}

func (x *Closer) CloseAll() {
	x.once.Do(func() {
		x.mutex.Lock()
		errs := make(chan error, len(x.closersFuncs))
		for _, f := range x.closersFuncs {
			go func(f func() error) {
				errs <- f()
			}(f)
		}
		x.mutex.Unlock()

		for i := 0; i < cap(errs); i++ {
			if err := <-errs; err != nil {
				x.logger.Error(errors.Wrap(err, "closer error").Error())
			}
		}
	})
}
