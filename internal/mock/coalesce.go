package mock

// coalesce returns the first value from the input that is not equal to the zero value of type T.
// If all values are zero values, it returns the zero value of type T.
func coalesce[T comparable](v ...T) T {
	var z T
	for _, v := range v {
		if v != z {
			return v
		}
	}
	return z
}
