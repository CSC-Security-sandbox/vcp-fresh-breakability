package leakedresources

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ccfe"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/detectors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// Pipeline runs the leaked resources flow: for each registered detector, run Detect,
// aggregate all leak records, then call the reporter.
type Pipeline struct {
	detectors []model.Detector
	reporter  Reporter
}

// NewPipeline returns a pipeline with the default log reporter. Call RegisterDetector
// to add resource-specific detectors (pool, volume, snapshot, etc.).
func NewPipeline() *Pipeline {
	return &Pipeline{
		detectors: nil,
		reporter:  LogReporter{},
	}
}

// RegisterDetector adds a detector. Detectors are run in registration order.
func (p *Pipeline) RegisterDetector(d model.Detector) {
	if d == nil {
		return
	}
	p.detectors = append(p.detectors, d)
}

// SetReporter sets the reporter (e.g. to swap in a GCS reporter). Default is LogReporter.
func (p *Pipeline) SetReporter(r Reporter) {
	if r != nil {
		p.reporter = r
	}
}

// Run executes all registered detectors and then reports the aggregated leak records.
// It is invoked by the cron-triggered locked task in core/app.go.
func (p *Pipeline) Run(ctx context.Context, storage database.Storage) error {
	logger := util.GetLogger(ctx)
	logger.Info("Leaked resources pipeline started")

	var all []model.LeakRecord
	for _, d := range p.detectors {
		records, err := d.Detect(ctx, storage)
		if err != nil {
			logger.Errorf("Leaked resources detector %s failed: %v", d.Name(), err)
			continue
		}
		all = append(all, records...)
	}

	if err := p.reporter.Report(ctx, all); err != nil {
		logger.Errorf("Leaked resources monitoring failed: %v", err)
		return err
	}

	logger.Info("Leaked resources pipeline finished")
	return nil
}

// Run executes the default pipeline with registered detectors (pool CCFE vs VCP; volume, snapshot in follow-up).
// Invoked by the cron in core/app.go.
func Run(ctx context.Context, storage database.Storage) error {
	p := NewPipeline()
	ccfeClient := ccfe.NewClient(auth.GenerateCallbackToken)
	p.RegisterDetector(detectors.NewPoolDetector(ccfeClient))
	p.RegisterDetector(detectors.NewVolumeOrphanDetector())
	p.RegisterDetector(detectors.NewSnapshotOrphanDetector())
	return p.Run(ctx, storage)
}
