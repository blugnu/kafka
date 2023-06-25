package mock

func coalesce[T comparable](v ...T) T {
	var z T
	for _, v := range v {
		if v != z {
			return v
		}
	}
	return z
}
