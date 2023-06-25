package kafka

import (
	"errors"
	"testing"
)

func TestError(t *testing.T) {
	// ARRANGE
	sut := Error("message")

	// ACT
	got := sut.Error()

	// ASSERT
	wanted := "message"
	if wanted != got {
		t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
	}
}

func TestConfigurationError(t *testing.T) {
	// ARRANGE
	inerr := Error("inner error")
	sut := ConfigurationError{inerr}

	t.Run("error", func(t *testing.T) {
		// ACT
		got := sut.Error()

		// ASSERT
		wanted := "configuration error: inner error"
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("is", func(t *testing.T) {
		// ARRANGE
		testcases := []struct {
			name   string
			target error
			result bool
		}{
			{name: "identical", target: sut, result: true},
			{name: "equal", target: ConfigurationError{inerr}, result: true},
			{name: "wraps nil", target: ConfigurationError{nil}, result: true},
			{name: "wraps matching error", target: ConfigurationError{Error("inner error")}, result: true},
			{name: "wraps non-matching error", target: ConfigurationError{errors.New("inner error")}, result: false},
			{name: "not fatal", target: errors.New("not fatal"), result: false},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				got := sut.Is(tc.target)

				// ASSERT
				wanted := tc.result
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		}
	})

	t.Run("unwrap", func(t *testing.T) {
		// ACT
		got := sut.Unwrap()

		// ASSERT
		wanted := inerr
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestFatalError(t *testing.T) {
	// ARRANGE
	inerr := Error("inner error")
	sut := FatalError{inerr}

	t.Run("error", func(t *testing.T) {
		// ACT
		got := sut.Error()

		// ASSERT
		wanted := "fatal error: inner error"
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("is", func(t *testing.T) {
		// ARRANGE
		testcases := []struct {
			name   string
			target error
			result bool
		}{
			{name: "identical", target: sut, result: true},
			{name: "equal", target: FatalError{inerr}, result: true},
			{name: "wraps nil", target: FatalError{nil}, result: true},
			{name: "wraps matching error", target: FatalError{Error("inner error")}, result: true},
			{name: "wraps non-matching error", target: FatalError{errors.New("inner error")}, result: false},
			{name: "not fatal", target: errors.New("not fatal"), result: false},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				got := sut.Is(tc.target)

				// ASSERT
				wanted := tc.result
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		}
	})

	t.Run("unwrap", func(t *testing.T) {
		// ACT
		got := sut.Unwrap()

		// ASSERT
		wanted := inerr
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}

func TestUnexpectedDeliveryEvent(t *testing.T) {
	// ARRANGE
	sut := UnexpectedDeliveryEvent{csPanic}

	t.Run("error", func(t *testing.T) {
		// ACT
		got := sut.Error()

		// ASSERT
		wanted := "unexpected delivery event: csPanic"
		if wanted != got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})

	t.Run("is", func(t *testing.T) {
		// ARRANGE
		testcases := []struct {
			name   string
			target error
			result bool
		}{
			{name: "identical", target: sut, result: true},
			{name: "equal", target: UnexpectedDeliveryEvent{csPanic}, result: true},
			{name: "nil event", target: UnexpectedDeliveryEvent{}, result: true},
			{name: "other event", target: UnexpectedDeliveryEvent{csStopped}, result: false},
			{name: "different error", target: errors.New("other error"), result: false},
		}
		for _, tc := range testcases {
			t.Run(tc.name, func(t *testing.T) {
				// ACT
				got := sut.Is(tc.target)

				// ASSERT
				wanted := tc.result
				if wanted != got {
					t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
				}
			})
		}
	})
}
