package leakedresources

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ccfe"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/detectors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscalerleakedresources "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/leakedresources"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var newRegionalAddressLister = hyperscalerleakedresources.NewRegionalAddressLister

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
	logger.Infof("Leaked resources pipeline started (detectors=%d)", len(p.detectors))

	var all []model.LeakRecord
	for _, d := range p.detectors {
		logger.Infof("Leaked resources: checking detector=%s", d.Name())
		records, err := d.Detect(ctx, storage)
		if err != nil {
			logger.Errorf("Leaked resources detector %s failed: %v", d.Name(), err)
			continue
		}
		logger.Infof("Leaked resources: detector=%s completed, leaks_found=%d", d.Name(), len(records))
		all = append(all, records...)
	}
	if len(all) == 0 {
		logger.Info("Leaked resources: no leaks found in this run")
	}

	if err := p.reporter.Report(ctx, all); err != nil {
		logger.Errorf("Leaked resources monitoring failed: %v", err)
		return err
	}

	logger.Infof("Leaked resources pipeline finished (total_leaks=%d)", len(all))
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

	// Internal reserved IP detection is part of the same pipeline; enable/disable with LEAKED_RESOURCES_MONITORING_ENABLED (core/app.go).
	lister, err := newRegionalAddressLister(ctx)
	if err != nil {
		util.GetLogger(ctx).Warnf("Leaked resources: internal reserved IP detector skipped (compute lister): %v", err)
	} else {
		p.RegisterDetector(detectors.NewInternalReservedIPDetector(lister, detectors.DefaultInternalReservedIPMinAge()))
	}

	return p.Run(ctx, storage)
}
