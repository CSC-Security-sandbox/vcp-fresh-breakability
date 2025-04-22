package nillable

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToPointer(t *testing.T) {
	t.Run("String", func(tt *testing.T) {
		p := ToPointer("stringvalue")
		require.NotNil(tt, p)
		assert.Equal(tt, "stringvalue", *p)
	})
	t.Run("int", func(tt *testing.T) {
		p := ToPointer(int(100))
		require.NotNil(tt, p)
		assert.Equal(tt, 100, *p)
	})
	t.Run("int64", func(tt *testing.T) {
		p := ToPointer(int64(999))
		require.NotNil(tt, p)
		assert.Equal(tt, int64(999), *p)
	})
}

func TestFromPointer(t *testing.T) {
	t.Run("String", func(tt *testing.T) {
		var s = "stringvalue"
		p := FromPointer(&s)
		assert.Equal(tt, s, p)
		assert.Equal(tt, reflect.String, reflect.ValueOf(p).Kind())
	})
	t.Run("NilString", func(tt *testing.T) {
		var s *string
		p := FromPointer(s)
		assert.Empty(tt, p)
		assert.Equal(tt, reflect.String, reflect.ValueOf(p).Kind())
	})
	t.Run("int", func(tt *testing.T) {
		var i = 100
		p := FromPointer(&i)
		assert.Equal(tt, i, p)
		assert.Equal(tt, reflect.Int, reflect.ValueOf(p).Kind())
	})
	t.Run("NilInt", func(tt *testing.T) {
		var i *int
		p := FromPointer(i)
		assert.Empty(tt, p)
		assert.Equal(tt, reflect.Int, reflect.ValueOf(p).Kind())
	})
	t.Run("int64", func(tt *testing.T) {
		var i int64 = 100
		p := FromPointer(&i)
		assert.Equal(tt, i, p)
		assert.Equal(tt, reflect.Int64, reflect.ValueOf(p).Kind())
	})
	t.Run("NilInt64", func(tt *testing.T) {
		var i *int64
		p := FromPointer(i)
		assert.Empty(tt, p)
		assert.Equal(tt, reflect.Int64, reflect.ValueOf(p).Kind())
	})
}

func TestFromPointerWithFallback(t *testing.T) {
	t.Run("String", func(tt *testing.T) {
		var s = "stringvalue"
		p := FromPointerWithFallback(&s, "fallback")
		assert.Equal(tt, s, p)
		assert.Equal(tt, reflect.String, reflect.ValueOf(p).Kind())
	})
	t.Run("NilString", func(tt *testing.T) {
		var s *string
		p := FromPointerWithFallback(s, "fallback")
		assert.Equal(tt, "fallback", p)
		assert.Equal(tt, reflect.String, reflect.ValueOf(p).Kind())
	})
	t.Run("int", func(tt *testing.T) {
		var i = 100
		p := FromPointerWithFallback(&i, 999)
		assert.Equal(tt, i, p)
		assert.Equal(tt, reflect.Int, reflect.ValueOf(p).Kind())
	})
	t.Run("NilInt", func(tt *testing.T) {
		var i *int
		p := FromPointerWithFallback(i, 999)
		assert.Equal(tt, 999, p)
		assert.Equal(tt, reflect.Int, reflect.ValueOf(p).Kind())
	})
	t.Run("int64", func(tt *testing.T) {
		var i int64 = 100
		p := FromPointerWithFallback(&i, 999)
		assert.Equal(tt, i, p)
		assert.Equal(tt, reflect.Int64, reflect.ValueOf(p).Kind())
	})
	t.Run("NilInt64", func(tt *testing.T) {
		var i *int64
		p := FromPointerWithFallback(i, 999)
		assert.Equal(tt, int64(999), p)
		assert.Equal(tt, reflect.Int64, reflect.ValueOf(p).Kind())
	})
}

func TestToPointerArray(t *testing.T) {
	t.Run("String", func(tt *testing.T) {
		a := []string{"val1", "val2"}
		converted := ToPointerArray(a)
		require.Len(tt, converted, len(a))
		for i := 0; i < len(a); i++ {
			assert.Equal(tt, a[0], *converted[0])
		}
	})
	t.Run("WhenArrayEmpty", func(tt *testing.T) {
		a := []int64{}
		converted := ToPointerArray(a)
		require.Len(tt, converted, len(a))
		for i := 0; i < len(a); i++ {
			assert.Equal(tt, a[0], *converted[0])
		}
	})
	t.Run("int64", func(tt *testing.T) {
		a := []int64{111, 222}
		converted := ToPointerArray(a)
		require.Len(tt, converted, len(a))
		for i := 0; i < len(a); i++ {
			assert.Equal(tt, a[0], *converted[0])
		}
	})
}

func TestFromPointerArray(t *testing.T) {
	t.Run("String", func(tt *testing.T) {
		a := []string{"val1", "val2"}
		aa := ToPointerArray(a)
		var converted = FromPointerArray(aa)
		require.Len(tt, converted, len(aa))
		for i := 0; i < len(a); i++ {
			assert.Equal(tt, a[i], converted[i])
		}
	})
	t.Run("WhenArrayEmpty", func(tt *testing.T) {
		a := []int64{}
		aa := ToPointerArray(a)
		var converted = FromPointerArray(aa)
		require.Len(tt, converted, len(aa))
		for i := 0; i < len(a); i++ {
			assert.Equal(tt, a[i], converted[i])
		}
	})
	t.Run("WhenArrayContainsNil", func(tt *testing.T) {
		a := []int64{111, 222}
		aa := ToPointerArray(a)
		aa = append(aa, nil)
		var converted = FromPointerArray(aa)
		require.Len(tt, converted, len(aa))
		for i := 0; i < len(a); i++ {
			assert.Equal(tt, a[i], converted[i])
		}
		assert.Empty(tt, converted[len(a)])
	})
	t.Run("int", func(tt *testing.T) {
		a := []int{111, 222}
		aa := ToPointerArray(a)
		var converted = FromPointerArray(aa)
		require.Len(tt, converted, len(aa))
		for i := 0; i < len(a); i++ {
			assert.Equal(tt, a[0], converted[0])
		}
	})
	t.Run("int64", func(tt *testing.T) {
		a := []int64{111, 222}
		aa := ToPointerArray(a)
		var converted = FromPointerArray(aa)
		require.Len(tt, converted, len(aa))
		for i := 0; i < len(a); i++ {
			assert.Equal(tt, a[0], converted[0])
		}
	})
}
func TestToStringPtr(t *testing.T) {
	t.Run("nil string", func(tt *testing.T) {
		var input *string
		output := ToStringPtr(input)
		assert.Nil(tt, output)
	})
	t.Run("String", func(tt *testing.T) {
		in := "input"
		output := ToStringPtr(&in)
		assert.Equal(tt, in, *output)
	})
	t.Run("bool", func(tt *testing.T) {
		in := true
		output := ToStringPtr(&in)
		assert.Equal(tt, "true", *output)
		in = false
		output = ToStringPtr(&in)
		assert.Equal(tt, "false", *output)
	})
	t.Run("int", func(tt *testing.T) {
		in := 123
		output := ToStringPtr(&in)
		assert.Equal(tt, "123", *output)
	})
	t.Run("int64", func(tt *testing.T) {
		in := int64(123237827382)
		output := ToStringPtr(&in)
		assert.Equal(tt, "123237827382", *output)
	})
	t.Run("float32", func(tt *testing.T) {
		in := 3.12
		output := ToStringPtr(&in)
		assert.Equal(tt, "3.12", *output)
	})
}
