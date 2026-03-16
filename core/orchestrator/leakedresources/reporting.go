package leakedresources

import (
	"context"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// Reporter receives the aggregated leak records from the pipeline and reports them
// (e.g. logs, GCS). The pipeline calls Report once per run with all records.
type Reporter interface {
	Report(ctx context.Context, records []model.LeakRecord) error
}

// LogReporter writes leak records to the application logger (summary + one line per record).
type LogReporter struct{}

// Report logs a summary and each leak record.
func (LogReporter) Report(ctx context.Context, records []model.LeakRecord) error {
	logger := util.GetLogger(ctx)
	if len(records) == 0 {
		logger.Info("Leaked resources monitoring: no leaked resources detected")
		return nil
	}
	logger.Warnf("Leaked resources monitoring: found %d leaked resource(s)", len(records))
	for _, r := range records {
		msg := "  Leaked %s: id=%s name=%s project=%s region=%s reason=%s"
		args := []interface{}{r.ResourceType, r.ResourceID, r.ResourceName, r.ProjectID, r.Region, r.Reason}
		if len(r.Extra) > 0 {
			parts := make([]string, 0, len(r.Extra))
			for k, v := range r.Extra {
				parts = append(parts, k+"="+v)
			}
			msg += " %s"
			args = append(args, strings.Join(parts, " "))
		}
		logger.Warnf(msg, args...)
	}
	return nil
}
