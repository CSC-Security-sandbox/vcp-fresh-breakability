package entity

import (
	"encoding/json"
	"time"
)

// UnixNano the number of nanoseconds elapsed since January 1, 1970 UTC. see time.UnixNano
type UnixNano int64

const milliToNano = int64(time.Millisecond)
const layout = "2006-01-02T15:04:05.000Z"

// MarshalJSON when marshaling UnixNano to json it should be in the format yyyy-MM-DDThh:mm:ssZ instead of int64
func (c UnixNano) MarshalJSON() ([]byte, error) {
	tmp := time.Unix(0, int64(c)).UTC()
	return []byte("\"" + tmp.Format(layout) + "\""), nil
}

// UnmarshalJSON converts timestamps from JSON documents into the UnixNano type.
// It handles two specific formats:
// 1. ISO 8601 timestamps, as are found in responses from the ONTAP REST API.
// 2. Unix timestamps in milliseconds, as are found in messages from the Cloud Backup Service.
func (c *UnixNano) UnmarshalJSON(b []byte) error {
	var data interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}

	switch v := data.(type) {
	case string:
		t, err := time.Parse(layout, v)
		if err != nil {
			return err
		}
		*c = UnixNano(t.UnixNano())
	case float64:
		*c = MilliToNano(int64(v))
	}
	return nil
}

// ToTime converts the UnixNano value to time.Time
func (c UnixNano) ToTime() time.Time {
	return time.Unix(0, int64(c)).UTC()
}

// MilliToNano converts milliseconds to UnixNano
func MilliToNano(t int64) UnixNano {
	return UnixNano(t * milliToNano)
}
