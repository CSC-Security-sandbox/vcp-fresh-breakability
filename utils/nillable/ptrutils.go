package nillable

import "fmt"

// ToPointer returns a pointer to value.
// This is especially useful when initializing structs.
func ToPointer[T any](value T) *T {
	return &value
}

// FromPointer returns the value from valuePtr OR a new default value of the correct type
func FromPointer[T any](valuePtr *T) T {
	if valuePtr == nil {
		return *new(T)
	}
	return *valuePtr
}

// FromPointerWithFallback returns the value from valuePtr OR the provided fallback value
func FromPointerWithFallback[T any](valuePtr *T, fallbackValue T) T {
	if valuePtr == nil {
		return fallbackValue
	}
	return *valuePtr
}

// ToPointerArray converts an array of values to an array of pointer to value
func ToPointerArray[T any](values []T) []*T {
	pArray := make([]*T, 0)
	for _, value := range values {
		pArray = append(pArray, ToPointer(value))
	}
	return pArray
}

// FromPointerArray converts an array of pointer to value to an array of values
func FromPointerArray[T any](values []*T) []T {
	pArray := make([]T, 0)
	for _, value := range values {
		if value == nil {
			v := new(T)
			pArray = append(pArray, *v)
		} else {
			pArray = append(pArray, *value)
		}
	}
	return pArray
}

// ToStringPtr converts a pointer to a value to a pointer to a string formatted version of the value
func ToStringPtr[T any](valuePtr *T) *string {
	if valuePtr == nil {
		return nil
	}
	s := fmt.Sprintf("%v", *valuePtr)
	return &s
}
