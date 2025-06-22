package kafka

// addr returns the first non-empty string in values
func addr[T any](v T) *T {
	return &v
}

// coalesce returns the first value from values that is not the zero value for type T.
// If all values are zero values, it returns the zero value of type T.
func coalesce[T comparable](values ...T) T {
	var z T

	for _, value := range values {
		if value != z {
			return value
		}
	}

	return z
}
