package kafka

import "fmt"

type consumerstate int

const (
	csInitialising consumerstate = iota
	csReady
	csSubscribed
	csRunning
	csStopping
	csStopped
	csPanic
)

func (cs consumerstate) String() string {
	s, ok := map[consumerstate]string{
		csInitialising: "csInitialising",
		csReady:        "csReady",
		csSubscribed:   "csSubscribed",
		csRunning:      "csRunning",
		csStopping:     "csStopping",
		csStopped:      "csStopped",
		csPanic:        "csPanic",
	}[cs]
	if !ok {
		return fmt.Sprintf("invalid consumerstate: %d", cs)
	}
	return s
}
