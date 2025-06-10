package common

import (
	"time"
)

type EventType string

const (
	EventCreated   EventType = "Created"
	EventScheduled EventType = "Scheduled"
	EventError     EventType = "Error"
	EventOrphan    EventType = "Orphan Scheduled"
	EventDone      EventType = "Done"
	EventRequest   EventType = "Request"
)

type Event struct {
	Type       EventType
	Timestamp  time.Time
	Attributes map[string]any
}

type ErrorContainer struct {
	Code    float64
	Message string
}

type EventBase struct {
	Events []Event
	Error  ErrorContainer
}

func (base *EventBase) AddEvent(event Event) {
	base.Events = append(base.Events, event)
}

func NewEvent(eventType EventType, timestamp time.Time, fields ...Field) Event {
	returnEvent := Event{
		Type:       eventType,
		Timestamp:  timestamp,
		Attributes: map[string]any{},
	}
	returnEvent.AddFields(fields...)
	return returnEvent
}

func (event *Event) AddFields(fields ...Field) {
	for _, f := range fields {
		event.Attributes[f.Name()] = f.Value()
	}
}
