package kafka

import (
	"fmt"
	"time"
)

// RetryThrottle is the interface for retry throttling.
//
// It provides a single method that is called before each retry attempt
// to update the metadata for the next retry in a sequence.
type RetryThrottler interface {
	Update(*RetryMetadata) error // updates the metadata for the next retry in a sequence
}

// RetryThrottle is a retry throttler that may be configured to implement
// a variety of retry strategies.
type RetryThrottle struct {
	BackoffFactor float64       // the multiplier applied to Interval to calculate retry delays; set to <= 1 to disable (constant interval)
	Interval      time.Duration // the interval for the first retry; BackoffFactor is applied to determine intervals for any subsequent retries
	MaxInterval   time.Duration // the maximum interval between retries; set 0 to disable (equivalent to a BackoffFactor of 1.0)
	MaxRetries    int           // the maximum number of retries; set 0 to disable (unlimited retries; be careful!)
}

// String returns a string representation of the retry throttle configuration.
func (p RetryThrottle) String() string {
	bf := fmt.Sprintf("backoff_factor=%.2f ", p.BackoffFactor)
	md := fmt.Sprintf("max_interval=%s ", p.MaxInterval)
	mr := fmt.Sprintf("max_retries=%d", p.MaxRetries)

	if p.BackoffFactor <= 1.0 {
		bf = ""
	}
	if p.MaxInterval == 0 {
		md = ""
	}
	if p.MaxRetries == 0 {
		mr = "max_retries=<unlimited>"
	}

	return fmt.Sprintf("interval=%s %s%s%s", p.Interval, bf, md, mr)
}

// Update increments the retry count and calculates and updates the interval for the
// next retry.
//
// The throttle will set any maximum number of retries but the caller is responsible
// for checking whether the retry number exceeds the maximum after the throttle has been applied.
func (p *RetryThrottle) Update(retry *RetryMetadata) error {
	if retry.Max == 0 {
		retry.Max = p.MaxRetries
	}

	retry.Num++
	if retry.Num > retry.Max {
		return ErrRetryLimitReached
	}

	retry.Interval = p.Interval
	if p.MaxInterval == 0 || p.BackoffFactor <= 1.0 {
		return nil
	}

	for i := 1; i < retry.Num && retry.Interval < p.MaxInterval; i++ {
		// retry.Interval = time.Duration(retry.Interval.Seconds()*p.BackoffFactor) * time.Second
		retry.Interval = time.Duration(float64(retry.Interval) * p.BackoffFactor)
	}
	if retry.Interval > p.MaxInterval {
		retry.Interval = p.MaxInterval
	}

	return nil
}

// unthrottledThrottler is a retry throttler that does not apply any throttling to retries.
type unthrottledThrottler struct{}

// Update increments the retry count.
func (*unthrottledThrottler) Update(retry *RetryMetadata) error {
	retry.Num++
	return nil
}

// unthrottled is a singleton instance of unthrottledThrottler.
var unthrottled = &unthrottledThrottler{}

// UnthrottledRetries returns a retry throttler that does not apply any throttling to retries.
//
// A Retry using this throttler will retry immediately and indefinitely.
func UnthrottledRetries() RetryThrottler {
	return unthrottled
}
