package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateWeekdayPartOfScheduleName(t *testing.T) {
	assert.Equal(t, "sunday", GenerateWeekdayPartOfScheduleName([]int{0}))
	assert.Equal(t, "monday+tuesday", GenerateWeekdayPartOfScheduleName([]int{1, 2}))
	assert.Equal(t, "", GenerateWeekdayPartOfScheduleName([]int{}))
}

func TestGenerateHourPartOfScheduleName(t *testing.T) {
	assert.Equal(t, "12am", GenerateHourPartOfScheduleName([]int{}))
	assert.Equal(t, "12am", GenerateHourPartOfScheduleName([]int{0}))
	assert.Equal(t, "1am", GenerateHourPartOfScheduleName([]int{1}))
	assert.Equal(t, "12pm", GenerateHourPartOfScheduleName([]int{12}))
	assert.Equal(t, "1pm", GenerateHourPartOfScheduleName([]int{13}))
	assert.Equal(t, "11pm", GenerateHourPartOfScheduleName([]int{23}))
}

func TestGenerateMinutePartOfScheduleName(t *testing.T) {
	assert.Equal(t, "0-min-past", GenerateMinutePartOfScheduleName([]int{}))
	assert.Equal(t, "0-min-past", GenerateMinutePartOfScheduleName([]int{0}))
	assert.Equal(t, "15-min-past", GenerateMinutePartOfScheduleName([]int{15}))
	assert.Equal(t, "59-min-past", GenerateMinutePartOfScheduleName([]int{59}))
}
