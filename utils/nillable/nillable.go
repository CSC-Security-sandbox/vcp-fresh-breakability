package nillable

import (
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/go-openapi/strfmt"
)

// GetInstance returns the specified instance if it's not nil, otherwise it returns the specified default
func GetInstance(x, def interface{}) interface{} {
	if x != nil && !reflect.ValueOf(x).IsNil() {
		return x
	}
	return def
}

// GetString returns the specified string if it's not nil, otherwise it returns the specified default
func GetString(s *string, def string) string {
	if s != nil {
		return *s
	}
	return def
}

// GetBool returns the specified boolean if it's not nil, otherwise it returns the specified default
func GetBool(s *bool, def bool) bool {
	if s != nil {
		return *s
	}
	return def
}

// GetInt returns the specified integer if it's not nil, otherwise it returns the specified default
func GetInt(s *int, def int) int {
	if s != nil {
		return *s
	}
	return def
}

// GetInt16 returns the specified 16-bit integer if it's not nil, otherwise it returns the specified default
func GetInt16(s *int16, def int16) int16 {
	if s != nil {
		return *s
	}
	return def
}

// GetInt32 returns the specified 32-bit integer if it's not nil, otherwise it returns the specified default
func GetInt32(s *int32, def int32) int32 {
	if s != nil {
		return *s
	}
	return def
}

// GetInt64 returns the specified 64-bit integer if it's not nil, otherwise it returns the specified default
func GetInt64(s *int64, def int64) int64 {
	if s != nil {
		return *s
	}
	return def
}

// GetUint64 returns the specified unsigned 64-bit integer if it's not nil, otherwise it returns the specified default
func GetUint64(s *uint64, def uint64) uint64 {
	if s != nil {
		return *s
	}
	return def
}

// GetFloat64 returns the specified 64-bit float if it's not nil, otherwise it returns the specified default
func GetFloat64(s *float64, def float64) float64 {
	if s != nil {
		return *s
	}
	return def
}

// GetNilIfEmptyString returns the specified string if it's not empty, otherwise it returns nil
func GetNilIfEmptyString(s string) *string {
	if s != "" {
		return &s
	}
	return nil
}

// AllNilStrings returns true if all string pointers it gets passed are nil, false otherwise
func AllNilStrings(ptrs ...*string) bool {
	if len(ptrs) < 1 {
		return false
	}
	for _, ptr := range ptrs {
		if ptr != nil {
			return false
		}
	}
	return true
}

// AllNonNilStrings returns true if all string pointers it gets passed are non-nil, false otherwise
func AllNonNilStrings(ptrs ...*string) bool {
	for _, ptr := range ptrs {
		if ptr == nil {
			return false
		}
	}
	return true
}

// IsNilOrEmpty returns true if s is nil or points to an empty string
func IsNilOrEmpty(s *string) bool {
	return s == nil || *s == ""
}

// GetStringFromUUID returns the specified UUID as a string if it's not nil, otherwise it returns the specified default
func GetStringFromUUID(s *strfmt.UUID, def string) string {
	if s != nil {
		return string(*s)
	}
	return def
}

// GetNonNilStringSlice returns the slice of string if it's not nil, otherwise it returns a slice with a zero valued string
func GetNonNilStringSlice(slice []string) []string {
	return GetInstance(slice, []string{""}).([]string)
}

// GetBoolPtr Returns the pointer to the provided bool
func GetBoolPtr(value bool) *bool {
	return &value
}

// GetIntPtr Returns the pointer to the provided int
func GetIntPtr(value int) *int {
	return &value
}

// GetInt16Ptr Returns the pointer to the provided int16
func GetInt16Ptr(value int16) *int16 {
	return &value
}

// GetInt32Ptr Returns the pointer to the provided int32
func GetInt32Ptr(value int32) *int32 {
	return &value
}

// GetInt64Ptr Returns the pointer to the provided int64
func GetInt64Ptr(value int64) *int64 {
	return &value
}

// GetUInt64Ptr Returns the pointer to the provided uint64
func GetUInt64Ptr(value uint64) *uint64 {
	return &value
}

// GetStringPtr Returns the pointer to the provided string
func GetStringPtr(value string) *string {
	return &value
}

// GetTimePtr Returns the pointer to the provided time.Time
func GetTimePtr(value time.Time) *time.Time {
	return &value
}

// GetFloat64Ptr Returns the pointer to the provided float64
func GetFloat64Ptr(value float64) *float64 {
	return &value
}

// GetPtr Returns a pointer to the provided value. Can replace a lot of the specialized routines above.
func GetPtr[T any](value T) *T {
	return &value
}

// ConvertIntPointerToInt32Pointer converts int pointer to int32 pointer
func ConvertIntPointerToInt32Pointer(value *int) *int32 {
	if value == nil {
		return nil
	}
	temp := int32(*value)
	return &temp
}

// ConvertIntPointerToInt64Pointer converts int pointer to int64 pointer
func ConvertIntPointerToInt64Pointer(value *int) *int64 {
	if value == nil {
		return nil
	}
	temp := int64(*value)
	return &temp
}

// ConvertStringToInt64Ptr convert a string to int64 pointer
func ConvertStringToInt64Ptr(s string) (*int64, error) {
	i64, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, err
	}

	return &i64, nil
}

// ConvertStringToBoolPtr convert a string to bool pointer
func ConvertStringToBoolPtr(s string) (*bool, error) {
	boolean, err := strconv.ParseBool(s)
	if err != nil {
		return nil, err
	}

	return &boolean, nil
}

// ConvertInt64PtrToString convert an int64 pointer to string. Return default string value on nil
func ConvertInt64PtrToString(i64 *int64, def string) string {
	if i64 == nil {
		return def
	}

	return strconv.FormatInt(*i64, 10)
}

// ConvertBoolPtrToString convert a bool pointer to string. Return default string value on nil
func ConvertBoolPtrToString(b *bool, def string) string {
	if b == nil {
		return def
	}

	return strconv.FormatBool(*b)
}

// ConvertInt32PtrToIntPtr converts an int32 pointer to an int pointer
func ConvertInt32PtrToIntPtr(i32 *int32) *int {
	if i32 == nil {
		return nil
	}

	temp := int(*i32)
	return &temp
}

// ParseStringTimeTotimeTime parses a string in RFC3339 format to a time.Time pointer.
func ParseStringTimeTotimeTime(now string) (*time.Time, error) {
	if now == "" {
		return nil, nil
	}

	// layout := "2006-01-02 15:04:05"
	t, err := time.Parse(time.RFC3339, now)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

// Input: Duration string, format such as"PT1D23H45M59S"
// Output: seconds
func ParseDurationInSeconds(duration string) int64 {
	if duration == "" {
		return 0
	}
	days := getNumberFromStringFormatTime(duration, `[0-9]+D`)
	hours := getNumberFromStringFormatTime(duration, `[0-9]+H`)
	minutes := getNumberFromStringFormatTime(duration, `[0-9]+M`)
	seconds := getNumberFromStringFormatTime(duration, `[0-9]+S`)

	return int64(3600*24*days + 3600*hours + 60*minutes + seconds)
}

func getNumberFromStringFormatTime(duration string, pattern string) int {
	match := regexp.MustCompile(pattern).FindAllString(duration, -1)
	if match == nil {
		return 0
	}

	n := match[0][0 : len(match[0])-1]
	val, err := strconv.Atoi(n)
	if err != nil {
		return 0
	}

	return val
}
