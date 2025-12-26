package utils

import (
	"time"

	"github.com/avast/retry-go/v4"
)

// 注意：这里用的 v4 的包，调用者也要用 v4 包

func RetryFixedOpts(attempts uint, delay time.Duration, options ...retry.Option) []retry.Option {
	opts := []retry.Option{
		retry.Attempts(attempts),
		retry.Delay(delay),
		retry.DelayType(retry.FixedDelay),
	}
	opts = append(opts, options...)
	return opts
}
