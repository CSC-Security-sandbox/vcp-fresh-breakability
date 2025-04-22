package utils

import "math"

// GetConstraintInteger returns a constraint integer val if it falls between minimum and maximum value, otherwise returns default
func GetConstraintInteger[T ~int | ~uint](val, min, max, def T) T {
	if val < min || val > max {
		return def
	}
	return val
}

// ConstrainedCastUint64 returns an int64 cast of val,
// if val is greater than maxInt64 it returns maxInt64
func ConstrainedCastUint64(val uint64) int64 {
	if val > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(val)
}
