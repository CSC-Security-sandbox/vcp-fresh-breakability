package nillable

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
)

func TestGetInstance(t *testing.T) {
	type thingy struct{}
	def := &thingy{}
	t.Run("GetInstanceWithNonNilInstance", func(tt *testing.T) {
		x := &thingy{}
		if GetInstance(x, def) != x {
			tt.Fail()
		}
	})
	t.Run("WhenSpecifyingMixOfNilAndNonNilInstances", func(tt *testing.T) {
		var x *thingy
		if GetInstance(x, def) != def {
			tt.Fail()
		}
	})
	defSlice := []string{""}
	t.Run("WhenSpecifyingMixOfNilAndNonNilSlices", func(tt *testing.T) {
		var x []string
		if !reflect.DeepEqual(GetInstance(x, defSlice), defSlice) {
			tt.Fail()
		}
	})
	t.Run("WhenSpecifyingNonNilSlice", func(tt *testing.T) {
		var x = []string{"abc", "def"}
		if !reflect.DeepEqual(GetInstance(x, defSlice), x) {
			tt.Fail()
		}
	})
}

func TestGetString(t *testing.T) {
	def := "fallback"
	t.Run("GetStringWithNonNilString", func(tt *testing.T) {
		s := "expected"
		if GetString(&s, def) != s {
			tt.Fail()
		}
	})
	t.Run("GetStringWithNilString", func(tt *testing.T) {
		if GetString(nil, def) != def {
			tt.Fail()
		}
	})
}

func TestGetBool(t *testing.T) {
	def := false
	t.Run("GetBoolWithNonNilBool", func(tt *testing.T) {
		b := true
		if GetBool(&b, def) != b {
			tt.Fail()
		}
	})
	t.Run("GetBoolWithNilBool", func(tt *testing.T) {
		if GetBool(nil, def) != def {
			tt.Fail()
		}
	})
}

func TestGetInt(t *testing.T) {
	def := 1337
	t.Run("GetIntWithNonNilInt", func(tt *testing.T) {
		i := 54321
		if GetInt(&i, def) != i {
			tt.Fail()
		}
	})
	t.Run("GetIntWithNilInt", func(tt *testing.T) {
		if GetInt(nil, def) != def {
			tt.Fail()
		}
	})
}

func TestGetInt64(t *testing.T) {
	var def int64 = 1234934892384
	t.Run("GetInt64WithNonNilInt", func(tt *testing.T) {
		var i int64 = 3284234942342
		if GetInt64(&i, def) != i {
			tt.Fail()
		}
	})
	t.Run("GetInt64WithNilInt", func(tt *testing.T) {
		if GetInt64(nil, def) != def {
			tt.Fail()
		}
	})
}

func TestGetUint64(t *testing.T) {
	var def uint64 = 1234934892384
	t.Run("GetUint64WithNonNilInt", func(tt *testing.T) {
		var i uint64 = 3284234942342
		if GetUint64(&i, def) != i {
			tt.Fail()
		}
	})
	t.Run("GetUint64WithNilInt", func(tt *testing.T) {
		if GetUint64(nil, def) != def {
			tt.Fail()
		}
	})
}

func TestGetFloat64(t *testing.T) {
	var def = 1234934892384.12
	t.Run("GetFloat64WithNonNilInt", func(tt *testing.T) {
		var i = 3284234942342.21
		if GetFloat64(&i, def) != i {
			tt.Fail()
		}
	})
	t.Run("GetFloat64WithNilInt", func(tt *testing.T) {
		if GetFloat64(nil, def) != def {
			tt.Fail()
		}
	})
}

func TestGetNilIfEmptyString(t *testing.T) {
	t.Run("GetNilIfEmptyStringWithNonEmptyString", func(tt *testing.T) {
		if GetNilIfEmptyString("non-empty") == nil {
			tt.Fail()
		}
	})
	t.Run("GetNilIfEmptyStringWithEmptyString", func(tt *testing.T) {
		if GetNilIfEmptyString("") != nil {
			tt.Fail()
		}
	})
}

func TestAllNilStrings(t *testing.T) {
	nonNil := ""
	t.Run("TestAllNilStringsWithNoStrings", func(tt *testing.T) {
		if AllNilStrings() {
			tt.Fail()
		}
	})
	t.Run("TestAllNilStringsWithSingleNilString", func(tt *testing.T) {
		if !AllNilStrings(nil) {
			tt.Fail()
		}
	})
	t.Run("TestAllNilStringsWithSingleNonNilString", func(tt *testing.T) {
		if AllNilStrings(&nonNil) {
			tt.Fail()
		}
	})
	t.Run("TestAllNilStringsWithOnlyNilStrings", func(tt *testing.T) {
		if !AllNilStrings(nil, nil, nil, nil, nil) {
			tt.Fail()
		}
	})
	t.Run("TestAllNilStringsWithOnlyNonNilStrings", func(tt *testing.T) {
		if AllNilStrings(&nonNil, &nonNil, &nonNil, &nonNil, &nonNil) {
			tt.Fail()
		}
	})
	t.Run("TestAllNilStringsWithMixOfNilAndNonNilStrings", func(tt *testing.T) {
		if AllNilStrings(&nonNil, nil, nil) {
			tt.Fail()
		}
		if AllNilStrings(nil, &nonNil, nil) {
			tt.Fail()
		}
		if AllNilStrings(nil, nil, &nonNil) {
			tt.Fail()
		}
	})
}

func TestNoNilStrings(t *testing.T) {
	nonNil := ""
	t.Run("TestNoNilStringsWithNoStrings", func(tt *testing.T) {
		if !AllNonNilStrings() {
			tt.Fail()
		}
	})
	t.Run("TestNoNilStringsWithSingleNilString", func(tt *testing.T) {
		if AllNonNilStrings(nil) {
			tt.Fail()
		}
	})
	t.Run("TestNoNilStringsWithSingleNonNilString", func(tt *testing.T) {
		if !AllNonNilStrings(&nonNil) {
			tt.Fail()
		}
	})
	t.Run("TestNoNilStringsWithOnlyNilStrings", func(tt *testing.T) {
		if AllNonNilStrings(nil, nil, nil, nil, nil) {
			tt.Fail()
		}
	})
	t.Run("TestNoNilStringsWithOnlyNonNilStrings", func(tt *testing.T) {
		if !AllNonNilStrings(&nonNil, &nonNil, &nonNil, &nonNil, &nonNil) {
			tt.Fail()
		}
	})
	t.Run("TestNoNilStringsWithMixOfNilAndNonNilStrings", func(tt *testing.T) {
		if AllNonNilStrings(nil, &nonNil, &nonNil) {
			tt.Fail()
		}
		if AllNonNilStrings(&nonNil, nil, &nonNil) {
			tt.Fail()
		}
		if AllNonNilStrings(&nonNil, &nonNil, nil) {
			tt.Fail()
		}
	})
}

func TestIsNilOrEmpty(t *testing.T) {
	t.Run("IsNilOrEmptyWithNonEmptyString", func(tt *testing.T) {
		nonEmpty := "non-empty"
		if IsNilOrEmpty(&nonEmpty) {
			tt.Fail()
		}
	})
	t.Run("IsNilOrEmptyWithEmptyString", func(tt *testing.T) {
		empty := ""
		if !IsNilOrEmpty(&empty) {
			tt.Fail()
		}
	})
	t.Run("IsNilOrEmptyWithNil", func(tt *testing.T) {
		if !IsNilOrEmpty(nil) {
			tt.Fail()
		}
	})
}

func TestGetStringFromUUID(t *testing.T) {
	def := "fallback"
	t.Run("GetStringFromUUIDWithUUIDString", func(tt *testing.T) {
		expected := "dd7d85f8-1ff1-b40a-5903-fd7d5edbafcf"
		s := strfmt.UUID(expected)
		if GetStringFromUUID(&s, def) != expected {
			tt.Fail()
		}
	})
	t.Run("GetStringFromUUIDWithNilString", func(tt *testing.T) {
		if GetStringFromUUID(nil, def) != def {
			tt.Fail()
		}
	})
}

func TestGetBoolPtr(t *testing.T) {
	// Arrange
	mockBool := true

	// Act
	actualPtr := GetBoolPtr(true)

	// Assert
	assert.Equal(t, &mockBool, actualPtr)
}

func TestGetIntPtr(t *testing.T) {
	mockInt := 123

	actualPtr := GetIntPtr(mockInt)
	assert.Equal(t, &mockInt, actualPtr)
}

func TestGetInt16Ptr(t *testing.T) {
	mockInt16 := int16(123)
	actualPtr := GetInt16Ptr(mockInt16)
	assert.Equal(t, &mockInt16, actualPtr)
}

func TestGetInt32Ptr(t *testing.T) {
	// Arrange
	mockInt32 := int32(123)

	// Act
	actualPtr := GetInt32Ptr(mockInt32)

	// Assert
	assert.Equal(t, &mockInt32, actualPtr)
}

func TestGetInt64Ptr(t *testing.T) {
	// Arrange
	mockInt64 := int64(123)

	// Act
	actualPtr := GetInt64Ptr(mockInt64)

	// Assert
	assert.Equal(t, &mockInt64, actualPtr)
}

func TestGetUInt64Ptr(t *testing.T) {
	mockUInt64 := uint64(123)
	actualPtr := GetUInt64Ptr(mockUInt64)
	assert.Equal(t, &mockUInt64, actualPtr)
}

func TestGetStringPtr(t *testing.T) {
	mockString := "mockString"
	assert.Equal(t, &mockString, GetStringPtr(mockString))
}

func TestGetTimePtr(t *testing.T) {
	mockTime := time.Now()
	assert.Equal(t, &mockTime, GetTimePtr(mockTime))
}

func TestGetFloat64Ptr(t *testing.T) {
	mockFloat64 := 1231241224.12
	assert.Equal(t, &mockFloat64, GetFloat64Ptr(mockFloat64))
}

// Spot check a few types
func TestGetPtr(t *testing.T) {
	mockFloat64 := 1231241224.12
	assert.Equal(t, &mockFloat64, GetPtr(mockFloat64))

	mockString := "mockString"
	assert.Equal(t, &mockString, GetPtr(mockString))

	mockTime := time.Now()
	assert.Equal(t, &mockTime, GetPtr(mockTime))
}

func TestConvertIntPointerToInt32Pointer(t *testing.T) {
	t.Run("GetInt64PointerWithNonNilInt", func(tt *testing.T) {
		i := 3284234942342
		result := ConvertIntPointerToInt32Pointer(&i)
		if int(*result) != i && int32(i) != *result {
			tt.Fail()
		}
	})
	t.Run("GetInt32PointerWithNilInt", func(tt *testing.T) {
		if ConvertIntPointerToInt32Pointer(nil) != nil {
			tt.Fail()
		}
	})
}

func TestConvertIntPointerToInt64Pointer(t *testing.T) {
	t.Run("GetInt64PointerWithNonNilInt", func(tt *testing.T) {
		i := 3284234942342
		result := ConvertIntPointerToInt64Pointer(&i)
		if int(*result) != i && int64(i) != *result {
			tt.Fail()
		}
	})
	t.Run("GetInt64PointerWithNilInt", func(tt *testing.T) {
		if ConvertIntPointerToInt64Pointer(nil) != nil {
			tt.Fail()
		}
	})
}

func TestConvertStringToInt64Ptr(t *testing.T) {
	t.Run("WhenParseIntFails", func(tt *testing.T) {
		result, err := ConvertStringToInt64Ptr("this is not ibiza")
		if assert.Error(tt, err) {
			assert.Equal(tt, errors.New("strconv.ParseInt: parsing \"this is not ibiza\": invalid syntax").Error(), err.Error())
		}
		assert.Nil(tt, result)
	})
	t.Run("WhenSucceeds", func(tt *testing.T) {
		result, err := ConvertStringToInt64Ptr("64")
		assert.Nil(tt, err)
		if assert.NotNil(tt, result) {
			assert.Equal(tt, int64(64), *result)
		}
	})
}

func TestConvertStringToBoolPtr(t *testing.T) {
	t.Run("WhenParseBoolFails", func(tt *testing.T) {
		result, err := ConvertStringToBoolPtr("this is not ibiza")
		if assert.Error(tt, err) {
			assert.Equal(tt, errors.New("strconv.ParseBool: parsing \"this is not ibiza\": invalid syntax").Error(), err.Error())
		}
		assert.Nil(tt, result)
	})
	t.Run("WhenSucceeds", func(tt *testing.T) {
		result, err := ConvertStringToBoolPtr("true")
		assert.Nil(tt, err)
		if assert.NotNil(tt, result) {
			assert.True(tt, *result)
		}
	})
}

func TestConvertInt64PtrToString(t *testing.T) {
	t.Run("WhenNil", func(tt *testing.T) {
		expected := "this is not ibiza"
		result := ConvertInt64PtrToString(nil, expected)
		assert.Equal(tt, expected, result)
	})
	t.Run("WhenSucceeds", func(tt *testing.T) {
		i64 := int64(64)
		expected := "64"
		result := ConvertInt64PtrToString(&i64, "something else")
		assert.Equal(tt, expected, result)
	})
}

func TestConvertBoolPtrToString(t *testing.T) {
	t.Run("WhenNil", func(tt *testing.T) {
		expected := "this is not ibiza"
		result := ConvertBoolPtrToString(nil, expected)
		assert.Equal(tt, expected, result)
	})
	t.Run("WhenSucceeds", func(tt *testing.T) {
		b := true
		expected := "true"
		result := ConvertBoolPtrToString(&b, "something else")
		assert.Equal(tt, expected, result)
	})
}

func TestGetNonNilStringSlice(t *testing.T) {
	t.Run("WhenSliceIsNil", func(tt *testing.T) {
		result := GetNonNilStringSlice(nil)
		assert.Equal(tt, []string{""}, result)
	})

	t.Run("WhenSliceIsEmpty", func(tt *testing.T) {
		result := GetNonNilStringSlice([]string{})
		assert.Equal(tt, []string{}, result)
	})

	t.Run("WhenSliceHasValues", func(tt *testing.T) {
		input := []string{"value1", "value2"}
		result := GetNonNilStringSlice(input)
		assert.Equal(tt, input, result)
	})
}

func TestGetInt32(t *testing.T) {
	var def int32 = 1234934892
	t.Run("GetInt32WithNonNilInt", func(tt *testing.T) {
		var i int32 = 1234934892
		if GetInt32(&i, def) != i {
			tt.Fail()
		}
	})
	t.Run("GetInt32WithNilInt", func(tt *testing.T) {
		if GetInt32(nil, def) != def {
			tt.Fail()
		}
	})
}

func TestConvertInt32PtrToIntPtr(t *testing.T) {
	t.Run("WhenInputIsNil", func(tt *testing.T) {
		result := ConvertInt32PtrToIntPtr(nil)
		assert.Nil(tt, result)
	})

	t.Run("WhenInputIsNonNil", func(tt *testing.T) {
		var input int32 = 42
		result := ConvertInt32PtrToIntPtr(&input)
		assert.NotNil(tt, result)
		assert.Equal(tt, int(input), *result)
	})
}

func TestParseStringTimeTotimeTime(t *testing.T) {
	t.Run("WhenValidStringTime", func(tt *testing.T) {
		timeInString := "2025-06-04T19:09:00+00:00"
		_, err := ParseStringTimeTotimeTime(timeInString)
		assert.EqualValues(tt, err, nil)
	})
	t.Run("WhenInValidStringTime", func(tt *testing.T) {
		timeInString := "2025-06-04D19:09:00+00:00"
		_, err := ParseStringTimeTotimeTime(timeInString)
		assert.NotNil(tt, err, nil)
	})
	t.Run("WhenStringTimeIsEmpty", func(tt *testing.T) {
		timeInString := ""
		_, err := ParseStringTimeTotimeTime(timeInString)
		assert.EqualValues(tt, err, nil)
	})
}

func TestParseDuration(t *testing.T) {
	t.Run("ParseDurationInSeconds", func(tt *testing.T) {
		duration := "PT1D23H45M59S"
		expected := int64(171959)
		if ParseDurationInSeconds(duration) != expected {
			tt.Fail()
		}
	})
	t.Run("ParseDurationInSecondsEmptyString", func(tt *testing.T) {
		if ParseDurationInSeconds("") != 0 {
			tt.Fail()
		}
	})
}

func TestGetNumberFromStringFormatTime(t *testing.T) {
	t.Run("WhenMatchFound", func(tt *testing.T) {
		duration := "PT1D23H45M59S"
		pattern := `[0-9]+D`
		m := getNumberFromStringFormatTime(duration, pattern)
		assert.EqualValues(tt, m, 1)
	})
	t.Run("WhenMatchNotFound", func(tt *testing.T) {
		// duration has no 'D' character, hence no match will be found
		duration := "PT1T23H45M59S"
		pattern := `[0-9]+D`
		m := getNumberFromStringFormatTime(duration, pattern)
		if m != 0 {
			tt.Fail()
		}
	})
	t.Run("WhenErrorInParsingInt", func(tt *testing.T) {
		duration := "PT1D23H45M59S"
		// pattern to get non-numeric in above "duration" string, to get the ParseInt error
		pattern := `[A-Z]+T`
		m := getNumberFromStringFormatTime(duration, pattern)
		if m != 0 {
			tt.Fail()
		}
	})
}
