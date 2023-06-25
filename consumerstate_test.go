package kafka

import (
	"fmt"
	"testing"
)

func TestConsumerState(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		consumerstate
		string
	}{
		{csInitialising, "csInitialising"},
		{csReady, "csReady"},
		{csSubscribed, "csSubscribed"},
		{csRunning, "csRunning"},
		{csStopping, "csStopping"},
		{csStopped, "csStopped"},
		{csPanic, "csPanic"},
		{consumerstate(-1), "invalid consumerstate: -1"},
	}
	for tn, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tn), func(t *testing.T) {
			// ACT
			got := tc.consumerstate.String()

			// ASSERT
			wanted := tc.string
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}
