package utils

import (
	"fmt"
	"strings"
	"time"
)

func GenerateWeekdayPartOfScheduleName(daysOfWeek []int) (days string) {
	for i, day := range daysOfWeek {
		if i > 0 {
			days += "+"
		}
		days += strings.ToLower(time.Weekday(day).String())
	}
	return
}

func GenerateHourPartOfScheduleName(hours []int) string {
	hour := 12
	ampm := "am"
	if len(hours) > 0 {
		hour = hours[0]
		if hour > 11 {
			hour = hour - 12
			ampm = "pm"
		}
		if hour == 0 {
			hour = 12
		}
	}
	return fmt.Sprintf("%v%s", hour, ampm)
}

func GenerateMinutePartOfScheduleName(minutes []int) string {
	minute := 0
	if len(minutes) > 0 {
		minute = minutes[0]
	}
	return fmt.Sprintf("%v-min-past", minute)
}
