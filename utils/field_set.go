package utils

// BuildFieldSet converts a list of string-like field enums into a lookup set.
// A nil result preserves existing batch endpoint behavior for "no fields requested".
func BuildFieldSet[T ~string](fields []T) map[string]bool {
	if len(fields) == 0 {
		return nil
	}

	fieldSet := make(map[string]bool, len(fields))
	for _, field := range fields {
		fieldSet[string(field)] = true
	}

	return fieldSet
}
