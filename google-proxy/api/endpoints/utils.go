package api

import (
	"fmt"
	"regexp"
)

const uuidPattern = `^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`

var uuidRegex = regexp.MustCompile(uuidPattern)

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

func validateUUIDList(uuids []string, fieldName string) string {
	for i, u := range uuids {
		if !uuidRegex.MatchString(u) {
			return fmt.Sprintf("%s.%d in body should match '%s'", fieldName, i, uuidPattern)
		}
	}
	return ""
}
