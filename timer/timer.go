package timer

import (
	"context"
	"time"

	"github.com/pianoyeg94/go-zookeeper-leader-election/ctxt"
)

const (
	uvinf                     = 0x7FF0000000000000
	maxDuration time.Duration = 1<<63 - 1
)

func NewTimer(d time.Duration) *Timer {
	return &Timer{
		Timer: time.NewTimer(d),
	}
}

type Timer struct {
	*time.Timer
}

func (t *Timer) Stop() bool {
	wasActive := t.Timer.Stop()
	if !wasActive {
		select {
		case <-t.C:
		default:
		}
	}
	return wasActive
}

func (t *Timer) Reset(delay time.Duration) bool {
	t.Stop()
	return t.Timer.Reset(delay)
}

func RetryOverTime(
	ctx context.Context,
	delay time.Duration,
	backoff time.Duration,
	maxBackoff time.Duration,
	fn func(ctx context.Context) error,
	errh func(ctx context.Context, err error) bool,
) (err error) {
	var timer *Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for !ctxt.ContextDone(ctx) {
		if err = fn(ctx); err == nil {
			return nil
		}

		if errh != nil && !errh(ctx, err) {
			return err
		}

		if timer != nil {
			timer.Reset(delay)
		}

		if timer == nil {
			timer = NewTimer(delay)
		}

		select {
		case <-timer.C:
			if ctxt.ContextDone(ctx) {
				return err
			}
		case <-ctx.Done():
			return err
		}

		if delay += backoff; delay > maxBackoff {
			delay = maxBackoff
		}
	}

	if err == nil {
		err = ctx.Err()
	}

	return err
}
