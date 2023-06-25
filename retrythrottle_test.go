package kafka

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestRetryThrottle_String(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		delay      time.Duration
		factor     float64
		maxDelay   time.Duration
		maxRetries int
		string
	}{
		{delay: 2 * time.Second, factor: 1.5, maxDelay: 4 * time.Second, maxRetries: 5, string: "interval=2s backoff_factor=1.50 max_interval=4s max_retries=5"},
		{delay: 2 * time.Second, factor: 2, maxDelay: 3 * time.Second, maxRetries: 5, string: "interval=2s backoff_factor=2.00 max_interval=3s max_retries=5"},
		{delay: 120 * time.Second, factor: 1, maxRetries: 1, string: "interval=2m0s max_retries=1"},
		{delay: 120 * time.Second, factor: 1, string: "interval=2m0s max_retries=<unlimited>"},
	}
	for tn, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tn), func(t *testing.T) {
			// ARRANGE
			sut := &RetryThrottle{Interval: tc.delay, BackoffFactor: tc.factor, MaxInterval: tc.maxDelay, MaxRetries: tc.maxRetries}

			t.Run("stringer", func(t *testing.T) {
				// ACT
				got := sut.String()

				// ASSERT
				wanted := tc.string
				if wanted != got {
					t.Errorf("\nwanted %v\ngot    %v", wanted, got)
				}
			})
		})
	}
}

func TestExponentialBackoffUpdates(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		interval    time.Duration
		factor      float64
		maxInterval time.Duration
		maxRetries  int
		results     map[int]RetryMetadata
	}{
		{interval: 2 * time.Second, factor: 1.5, maxInterval: 4 * time.Second, maxRetries: 5,
			results: map[int]RetryMetadata{
				1: {Seq: 1, Num: 1, Max: 5, Interval: 2 * time.Second},
				2: {Seq: 1, Num: 2, Max: 5, Interval: 3 * time.Second},
				3: {Seq: 1, Num: 3, Max: 5, Interval: 4 * time.Second},
				4: {Seq: 1, Num: 4, Max: 5, Interval: 4 * time.Second},
				5: {Seq: 1, Num: 5, Max: 5, Interval: 4 * time.Second},
			},
		},
		{interval: 2 * time.Second, factor: 2, maxInterval: 16 * time.Second, maxRetries: 5,
			results: map[int]RetryMetadata{
				1: {Seq: 1, Num: 1, Max: 5, Interval: 2 * time.Second},
				2: {Seq: 1, Num: 2, Max: 5, Interval: 4 * time.Second},
				3: {Seq: 1, Num: 3, Max: 5, Interval: 8 * time.Second},
				4: {Seq: 1, Num: 4, Max: 5, Interval: 16 * time.Second},
				5: {Seq: 1, Num: 5, Max: 5, Interval: 16 * time.Second},
			},
		},
		{interval: 2 * time.Second, factor: 1, maxRetries: 5,
			results: map[int]RetryMetadata{
				1: {Seq: 1, Num: 1, Max: 5, Interval: 2 * time.Second},
				2: {Seq: 1, Num: 2, Max: 5, Interval: 2 * time.Second},
				3: {Seq: 1, Num: 3, Max: 5, Interval: 2 * time.Second},
				4: {Seq: 1, Num: 4, Max: 5, Interval: 2 * time.Second},
				5: {Seq: 1, Num: 5, Max: 5, Interval: 2 * time.Second},
			},
		},
		{interval: 120 * time.Second, maxRetries: 1,
			results: map[int]RetryMetadata{
				1: {Seq: 1, Num: 1, Max: 1, Interval: 120 * time.Second},
			},
		},
	}
	for tn, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tn), func(t *testing.T) {
			// ARRANGE
			sut := &RetryThrottle{
				Interval:      tc.interval,
				BackoffFactor: tc.factor,
				MaxInterval:   tc.maxInterval,
				MaxRetries:    tc.maxRetries,
			}
			metadata := RetryMetadata{Seq: 1}

			for i := 1; i <= len(tc.results); i++ {
				t.Run(fmt.Sprintf("retry: %d", i), func(t *testing.T) {
					// ACT
					err := sut.Update(&metadata)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}

					// ASSERT
					wanted := tc.results[i]
					got := metadata
					if wanted != got {
						t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
					}
				})
			}
		})
	}
}

func TestUnthrottledUpdates(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		retries int
	}{
		{1},
		{10},
		{100},
		{1000},
	}
	for tn, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tn), func(t *testing.T) {
			// ARRANGE
			sut := &unthrottledThrottler{}
			metadata := RetryMetadata{Seq: 1}

			// ACT
			for i := 1; i <= tc.retries; i++ {
				err := sut.Update(&metadata)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			// ASSERT
			wanted := RetryMetadata{
				Seq: 1,
				Num: tc.retries,
			}
			got := metadata
			if !reflect.DeepEqual(wanted, got) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}
