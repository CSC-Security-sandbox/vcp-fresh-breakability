package api

func DeduplicateSlice(elements []string) []string {
	uniqueElements := make(map[string]bool)
	var deduplicatedElements []string

	for _, element := range elements {
		if _, exists := uniqueElements[element]; !exists {
			uniqueElements[element] = true
			deduplicatedElements = append(deduplicatedElements, element)
		}
	}
	return deduplicatedElements
}
