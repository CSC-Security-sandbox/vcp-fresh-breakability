package sink

import (
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

const (
	dateLayout  = "2006-01-02"
	ContentType = "text/csv; charset=utf-8"
)

var (
	ReportName = env.GetString("BIZOPS_REPORT_NAME", "google_vsa_analytics_report")
	BucketName = env.GetString("BIZOPS_BUCKET_NAME", "harvest-farm-pv-au-se1")
	Region     = env.GetString("REGION", "au-se1")
)

func GetFilePath(date time.Time, timezone string) string {
	filename := fmt.Sprintf("%s-%s-%s.csv", date.Format(dateLayout), ReportName, timezone)
	return fmt.Sprintf("%s/%s", Region, filename)
}

func ValidateSinkParams(sinkParams *entity.BizopsSinkParams) error {
	if sinkParams == nil {
		return fmt.Errorf("sink params cannot be nil")
	}
	if sinkParams.Reader == nil {
		return fmt.Errorf("reader cannot be nil")
	}
	if sinkParams.Date.IsZero() {
		return fmt.Errorf("date cannot be zero")
	}
	if sinkParams.Timezone == "" {
		return fmt.Errorf("timezone cannot be empty")
	}
	return nil
}
