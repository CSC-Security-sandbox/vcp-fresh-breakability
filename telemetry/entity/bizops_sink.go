package entity

import (
	"io"
	"time"
)

type BizopsSinkParams struct {
	Reader   io.Reader
	SinkTime time.Time
	Region   string
	Date     time.Time
	Timezone string
}
