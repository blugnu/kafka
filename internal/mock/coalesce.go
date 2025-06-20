package mock

// coalesce returns the first non-zero value from the provided values.
func coalesce[T comparable](v ...T) T {
	var z T
	for _, v := range v {
		if v != z {
			return v
		}
	}
	return z
}
