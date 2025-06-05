package utils

import (
	"fmt"
	"reflect"
	"strings"
)

type FilterCondition struct {
	Field string
	Op    string
	Value interface{}
}

func NewFilterCondition() *FilterCondition {
	return &FilterCondition{}
}

func (fc *FilterCondition) WithConditions(field string, op string, value interface{}) *FilterCondition {
	fc.Field = field
	fc.Op = op
	fc.Value = value
	return fc
}

type Filter struct {
	Conditions []*FilterCondition
}

func CreateFilterWithConditions(filterConditions []*FilterCondition) *Filter {
	return &Filter{
		Conditions: filterConditions,
	}
}

func (f *Filter) Apply() [][]interface{} {
	return f.ToGORMQuery()
}

func (f *Filter) SetConditions(filterConditions []*FilterCondition) {
	f.Conditions = filterConditions
}

func (f *Filter) ToGORMQuery() [][]interface{} {
	var query [][]interface{}
	for _, condition := range f.Conditions {
		val := fmt.Sprintf("%s %s %v", condition.Field, condition.Op, condition.Value)
		if reflect.TypeOf(condition.Value).Kind() == reflect.String {
			val = fmt.Sprintf("%s %s '%s'", condition.Field, condition.Op, condition.Value)
		} else if reflect.TypeOf(condition.Value).Kind() == reflect.Slice {
			strSlice, _ := condition.Value.([]string)
			formattedString := FormatArrayForInCondition(strSlice)
			val = fmt.Sprintf("%s %s %s", condition.Field, condition.Op, formattedString)
		}
		query = append(query, []interface{}{val})
	}
	return query
}

func FormatArrayForInCondition(arr []string) string {
	formatted := make([]string, len(arr))
	for i, s := range arr {
		formatted[i] = fmt.Sprintf("'%s'", s)
	}
	result := strings.Join(formatted, ",")
	return fmt.Sprintf("(%s)", result)
}
