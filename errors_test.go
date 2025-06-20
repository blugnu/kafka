package kafka //nolint: testpackage // testing private members

import (
	"errors"
	"testing"

	"github.com/blugnu/test"
)

func TestConfigurationError(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "Error",
			exec: func(t *testing.T) {
				// ARRANGE
				err := errors.New("error")
				sut := ConfigurationError{err}

				// ACT
				result := sut.Error()

				// ASSERT
				test.That(t, result).Equals("configuration error: error")
			},
		},
		{scenario: "Is/identical",
			exec: func(t *testing.T) {
				// ARRANGE
				err := errors.New("error")
				sut := ConfigurationError{err}

				// ACT
				result := sut.Is(sut)

				// ASSERT
				test.IsTrue(t, result)
			},
		},
		{scenario: "Is/equal",
			exec: func(t *testing.T) {
				// ARRANGE
				err := errors.New("error")
				sut := ConfigurationError{err}

				// ACT
				result := sut.Is(ConfigurationError{err})

				// ASSERT
				test.IsTrue(t, result)
			},
		},
		{scenario: "Is/no error",
			exec: func(t *testing.T) {
				// ARRANGE
				err := errors.New("error")
				sut := ConfigurationError{err}

				// ACT
				result := sut.Is(ConfigurationError{})

				// ASSERT
				test.IsTrue(t, result)
			},
		},
		{scenario: "Is/different error",
			exec: func(t *testing.T) {
				// ARRANGE
				err := errors.New("same text != equal")
				sut := ConfigurationError{err}

				// ACT
				result := sut.Is(errors.New("same text != equal"))

				// ASSERT
				test.IsFalse(t, result)
			},
		},
		{scenario: "Unwrap",
			exec: func(t *testing.T) {
				// ARRANGE
				err := errors.New("error")
				sut := ConfigurationError{err}

				// ACT
				result := sut.Unwrap()

				// ASSERT
				test.That(t, result).Equals(err)
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
}

func TestConsumerPanicError(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		scenario string
		exec     func(t *testing.T)
	}{
		{scenario: "Error (zero-value)",
			exec: func(t *testing.T) {
				// ARRANGE
				sut := ConsumerPanicError{}

				// ACT
				result := sut.Error()

				// ASSERT
				test.That(t, result).Equals("consumer panic: recovered: <nil>")
			},
		},
		{scenario: "Error",
			exec: func(t *testing.T) {
				// ARRANGE
				sut := ConsumerPanicError{Recovered: "recovered value"}

				// ACT
				result := sut.Error()

				// ASSERT
				test.That(t, result).Equals("consumer panic: recovered: recovered value")
			},
		},
		{scenario: "Is",
			exec: func(t *testing.T) {
				// ARRANGE
				testcases := []struct {
					scenario string
					sut      ConsumerPanicError
					target   error
					result   bool
				}{
					{scenario: "target not a ConsumerPanicError",
						sut:    ConsumerPanicError{Recovered: "recovered value"},
						target: errors.New("other error"),
						result: false,
					},
					{scenario: "recovered, expecting recovered",
						sut:    ConsumerPanicError{Recovered: "recovered value"},
						target: ConsumerPanicError{Recovered: "recovered value"},
						result: true,
					},
					{scenario: "recovered, expecting other recovered",
						sut:    ConsumerPanicError{Recovered: "recovered value"},
						target: ConsumerPanicError{Recovered: "something else"},
						result: false,
					},
					{scenario: "recovered, expecting any ConsumerPanicError",
						sut:    ConsumerPanicError{Recovered: "recovered value"},
						target: ConsumerPanicError{},
						result: true,
					},
				}
				for _, tc := range testcases {
					t.Run(tc.scenario, func(t *testing.T) {
						// ACT
						result := errors.Is(tc.sut, tc.target)

						// ASSERT
						test.That(t, result).Equals(tc.result)
					})
				}
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.scenario, func(t *testing.T) {
			tc.exec(t)
		})
	}
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
