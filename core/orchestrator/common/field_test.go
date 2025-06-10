package common

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestField(t *testing.T) {
	t.Run("TestName", func(tt *testing.T) {
		name := "some_name"
		testField := String(name, "fieldValue")

		assert.Equal(tt, name, testField.Name())
	})
	t.Run("TestValue", func(tt *testing.T) {
		value := "some_value"
		testField := String("fieldName", value)

		assert.Equal(tt, value, testField.Value())
	})
	t.Run("TestType", func(tt *testing.T) {
		testField := String("fieldName", "fieldValue")

		assert.Equal(tt, TypeString, testField.Type())
	})
}

func TestCreateField(t *testing.T) {
	rs := func() string { return uuid.NewString() }

	t.Run("TestString", func(tt *testing.T) {
		key, val := rs(), rs()
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeString}, String(key, val))
	})
	t.Run("TestInt", func(tt *testing.T) {
		key, val := rs(), rand.Int()
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeInt}, Int(key, val))
	})
	t.Run("TestInt32", func(tt *testing.T) {
		key, val := rs(), rand.Int31()
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeInt32}, Int32(key, val))
	})
	t.Run("TestUInt", func(tt *testing.T) {
		key, val := rs(), uint(1)
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeUInt}, UInt(key, val))
	})
	t.Run("TestTime", func(tt *testing.T) {
		key, val := rs(), time.Unix(0, rand.Int63())
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeTime}, Time(key, val))
	})
	t.Run("TestDuration", func(tt *testing.T) {
		key, val := rs(), time.Duration(rand.Int63())
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeDuration}, Duration(key, val))
	})
	t.Run("TestStringer", func(tt *testing.T) {
		key, val := rs(), time.Unix(0, rand.Int63())
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeStringer}, Stringer(key, val))
	})
	t.Run("TestStringSlice", func(tt *testing.T) {
		key, val := rs(), []string{
			rs(), rs(), rs(),
		}
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeStringSlice}, StringSlice(key, val))
	})
	t.Run("TestError", func(tt *testing.T) {
		key, val := "error", fmt.Errorf("%s", rs())
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeError}, Error(val))
	})
	t.Run("TestNamedError", func(tt *testing.T) {
		key, val := rs(), fmt.Errorf("%s", rs())
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeError}, NamedError(key, val))
	})
	t.Run("TestNullTime", func(tt *testing.T) {
		key := rs()
		var val *time.Time
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeNullTime}, NullTime(key, val))
	})
	t.Run("TestAny", func(tt *testing.T) {
		type someStruct struct {
			val string
		}
		key, val := rs(), someStruct{val: rs()}
		assert.Equal(tt, Field{name: key, value: val, fieldType: TypeAny}, Any(key, val))
	})
}
