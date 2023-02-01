package utils

func Contains[T comparable](arr []T, item T) bool {
	for _, i := range arr {
		if i == item {
			return true
		}
	}

	return false
}

func Remove[T comparable](arr []T, item T) []T {
	result := []T{}

	for _, i := range arr {
		if i != item {
			result = append(result, i)
		}
	}

	return result
}
