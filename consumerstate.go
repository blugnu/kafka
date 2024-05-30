package kafka

import "fmt"

type consumerState int

const (
	csInitialising consumerState = iota
	csReady
	csSubscribed
	csRunning
	csStopping
	csStopped
	csPanic
)

func (cs consumerState) String() string {
	s, ok := map[consumerState]string{
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
