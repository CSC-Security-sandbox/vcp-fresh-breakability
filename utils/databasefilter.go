package utils

import (
	"fmt"
	"reflect"
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
		}
		query = append(query, []interface{}{val})
	}
	return query
}
