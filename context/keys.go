package context

type key int

const (
	kGroupId key = iota
	kMessageReceived
	kMessageProduced
	kOffset
)
