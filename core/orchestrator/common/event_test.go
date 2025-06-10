package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEvent(t *testing.T) {
	t.Run("TestNewEvent", func(tt *testing.T) {
		timestamp := time.Now()
		field1 := String("some_field", "val")

		expectedEvent := Event{
			Type:       EventCreated,
			Timestamp:  timestamp,
			Attributes: map[string]interface{}{field1.Name(): field1.Value()},
		}

		retEvent := NewEvent(EventCreated, timestamp, field1)

		assert.Equal(tt, expectedEvent, retEvent)
	})
	t.Run("TestAddFields", func(tt *testing.T) {
		timestamp := time.Now()
		field1 := String("some_field", "val")

		expectedEvent := Event{
			Type:       EventRequest,
			Timestamp:  timestamp,
			Attributes: map[string]interface{}{field1.Name(): field1.Value()},
		}

		retEvent := NewEvent(EventRequest, timestamp)
		retEvent.AddFields(field1)

		assert.Equal(tt, expectedEvent, retEvent)
	})
	t.Run("TestAddEvent'", func(tt *testing.T) {
		timestamp := time.Now()

		baseEvent := EventBase{}
		expectedEvent := NewEvent(EventRequest, timestamp)
		baseEvent.AddEvent(expectedEvent)

		assert.Equal(tt, expectedEvent, baseEvent.Events[0])
	})
}
