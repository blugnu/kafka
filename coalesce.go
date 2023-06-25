package kafka

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
