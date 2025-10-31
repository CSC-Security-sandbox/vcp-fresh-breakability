package utils

import (
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"time"
)

const (
	dateLayout = "2006-01-02"
	UTC        = "UTC"
	PST        = "PST"
)

var (
	PerformanceQueue  = "performance"
	UsageQueue        = "usage"
	CollectionQueue   = "collection"
	BizOpsReportQueue = "bizops"
)

type BizOpsReportParams struct {
	StartDate time.Time `json:"StartDate"`
	EndDate   time.Time `json:"EndDate"`
	TimeZone  string    `json:"TimeZone"`
	SinkType  string    `json:"SinkType"`
}

func ParseBizOpsReportParams(bizOpsReportParams *BizOpsReportParams) error {
	var timezone string
	switch bizOpsReportParams.TimeZone {
	case UTC:
		timezone = bizOpsReportParams.TimeZone
	case PST:
		timezone = "America/Los_Angeles"
	default:
		return fmt.Errorf("the time zone must be set to 'UTC' or 'PST' - received: '%s'", bizOpsReportParams.TimeZone)
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return fmt.Errorf("load location failure: %v", err)
	}
	if bizOpsReportParams.StartDate.IsZero() {
		bizOpsReportParams.StartDate = time.Now().Add(time.Hour * -24).In(location)
	}
	bizOpsReportParams.StartDate = bizOpsReportParams.StartDate.Truncate(time.Hour * 24)
	_, offset := bizOpsReportParams.StartDate.In(location).Zone()
	bizOpsReportParams.StartDate = bizOpsReportParams.StartDate.Add(time.Second * time.Duration(-offset))
	bizOpsReportParams.EndDate = bizOpsReportParams.StartDate.Add(time.Hour * 24)
	return nil
}

func ValidateResourceMetadata(resourceMetadata metadata.ResourceMetadata) error {
	if resourceMetadata.ResourceName == nil {
		return fmt.Errorf("ResourceName is nil")
	}
	if resourceMetadata.RegionName == nil {
		return fmt.Errorf("RegionName is nil")
	}
	if resourceMetadata.DeploymentName == nil {
		return fmt.Errorf("DeploymentName is nil")
	}
	return nil
}
