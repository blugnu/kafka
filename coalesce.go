package kafka

// addr returns the first non-empty string in values
func addr[T any](v T) *T {
	return &v
}

// coalesce returns the first non-zero value T in values
func coalesce[T comparable](values ...T) T {
	var z T
	for _, value := range values {
		if value != z {
			return value
		}
	}
	return z
}
