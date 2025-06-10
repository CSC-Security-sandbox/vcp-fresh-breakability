package common

import (
	"fmt"
	"time"
)

type Type string

const (
	TypeAny             Type = "FIELD_ANY"
	TypeDuration        Type = "FIELD_DURATION"
	TypeError           Type = "FIELD_ERROR"
	TypeString          Type = "FIELD_STRING"
	TypeStringer        Type = "FIELD_STRINGER"
	TypeStringSlice     Type = "FIELD_STRING_SLICE"
	TypeInt             Type = "FIELD_INT"
	TypeInt32           Type = "FIELD_INT32"
	TypeUInt            Type = "FIELD_UINT"
	TypeFloat64         Type = "FIELD_FLOAT64"
	TypeTime            Type = "FIELD_TIME"
	TypeNullTime        Type = "FIELD_NULL_TIME"
	TypeSensitiveString Type = "FIELD_SENSITIVE_STRING"
)

type Field struct {
	name      string
	value     interface{}
	fieldType Type
}

func (f *Field) Name() string {
	return f.name
}

func (f *Field) Value() interface{} {
	return f.value
}

func (f *Field) Type() Type {
	return f.fieldType
}

func String(name string, value string) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeString,
	}
}

func Stringer(name string, value fmt.Stringer) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeStringer,
	}
}

func StringSlice(name string, values []string) Field {
	return Field{
		name:      name,
		value:     values,
		fieldType: TypeStringSlice,
	}
}

func Int(name string, value int) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeInt,
	}
}

func Int32(name string, value int32) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeInt32,
	}
}

func UInt(name string, value uint) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeUInt,
	}
}

func Float64(name string, value float64) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeFloat64,
	}
}

func Any(name string, value interface{}) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeAny,
	}
}

func Duration(name string, value time.Duration) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeDuration,
	}
}

func Error(err error) Field {
	return Field{
		name:      "error",
		value:     err,
		fieldType: TypeError,
	}
}

func NamedError(name string, value error) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeError,
	}
}

func Time(name string, value time.Time) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeTime,
	}
}

func NullTime(name string, value *time.Time) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeNullTime,
	}
}

func SensitiveString(name string, value string) Field {
	return Field{
		name:      name,
		value:     value,
		fieldType: TypeSensitiveString,
	}
}
