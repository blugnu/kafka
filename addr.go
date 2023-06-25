package kafka

// addr returns an address of a specified value or nil if the value is
// the zero value of its type.
func addr[T comparable](v T) *T {
	var z T
	if v == z {
		return nil
	}
	return &v
}
