package leakedresources

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ccfe"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/detectors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/client"
)

// Pipeline runs the leaked resources flow: for each registered detector, run Detect,
// aggregate all leak records, then call the reporter.
type Pipeline struct {
	detectors []model.Detector
	reporter  Reporter
}

// NewPipeline returns a pipeline with default reporters (log + metrics) via MultiReporter.
// Call RegisterDetector to add resource-specific detectors (pool, volume, snapshot, etc.).
func NewPipeline() *Pipeline {
	return &Pipeline{
		detectors: nil,
		reporter:  NewMultiReporter(LogReporter{}, NewMetricsReporter()),
	}
}

// RegisterDetector adds a detector. Detectors are run in registration order.
func (p *Pipeline) RegisterDetector(d model.Detector) {
	if d == nil {
		return
	}
	p.detectors = append(p.detectors, d)
}

// SetReporter sets the reporter (e.g. to swap in a GCS reporter). Default is MultiReporter(LogReporter, MetricsReporter).
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
	runStatus := "success"
	defer func() {
		recordMonitoringRun(ctx, runStatus)
	}()

	var all []model.LeakRecord
	failedDetectors := 0
	for _, d := range p.detectors {
		logger.Infof("Leaked resources: checking detector=%s", d.Name())
		records, err := d.Detect(ctx, storage)
		if err != nil {
			logger.Errorf("Leaked resources detector %s failed: %v", d.Name(), err)
			failedDetectors++
			continue
		}
		logger.Infof("Leaked resources: detector=%s completed, leaks_found=%d", d.Name(), len(records))
		all = append(all, records...)
	}
	if len(all) == 0 {
		logger.Info("Leaked resources: no leaks found in this run")
	}

	if err := p.reporter.Report(ctx, all); err != nil {
		runStatus = "error"
		logger.Errorf("Leaked resources monitoring failed (total_leaks=%d, failed_detectors=%d): %v", len(all), failedDetectors, err)
		return err
	}
	if failedDetectors > 0 {
		runStatus = "partial_error"
	}

	logger.Infof("Leaked resources pipeline finished (total_leaks=%d, failed_detectors=%d, status=%s)", len(all), failedDetectors, runStatus)
	return nil
}

// Run executes the default pipeline with registered detectors (pool: live
// CCFE fetch via FetchCCFEPoolsWorkflow; volume, snapshot in follow-up).
// Invoked by the cron in core/app.go. The pool detector now triggers two
// Temporal workflows synchronously per tick — one GetRegionZonesWorkflow
// to enumerate zones in LOCAL_REGION and one FetchCCFEPoolsWorkflow per
// VCP account (which fans the per-(project, location) CCFE list calls out
// as parallel activities inside the workflow) — so CCFE data is always
// fresh and every zone in the region is diffed (including zones VCP has
// no rows in yet). There is no longer a clh_resources cache or a separate
// sync schedule. The CCFE client is still used directly by the
// BackupVaultDetector until that one is migrated to the same pattern.
func Run(ctx context.Context, storage database.Storage, temporalClient client.Client) error {
	p := NewPipeline()
	ccfeClient := ccfe.NewClient(auth.GenerateCallbackToken)
	ccfePoolFetcher := detectors.NewTemporalCCFEPoolFetcher(temporalClient)
	zoneFetcher := detectors.NewTemporalZoneFetcher(temporalClient)
	keyLister := detectors.NewProjectLocationLister(storage, zoneFetcher)
	p.RegisterDetector(detectors.NewPoolDetector(ccfePoolFetcher, keyLister))
	p.RegisterDetector(detectors.NewVolumeOrphanDetector())
	p.RegisterDetector(detectors.NewSnapshotOrphanDetector())
	p.RegisterDetector(detectors.NewBackupVaultDetector(ccfeClient))

	// Internal reserved IP detector: invokes GCE Compute API via Temporal workflow on worker pod
	p.RegisterDetector(detectors.NewInternalReservedIPDetector(detectors.DefaultInternalReservedIPMinAge()))

	// VM detector: invokes GCE Compute API via Temporal workflow, diffs VMs vs active pools
	p.RegisterDetector(detectors.NewVMDetector())

	// Disk detector: invokes GCE Compute API via Temporal workflow, diffs disks vs active pools
	p.RegisterDetector(detectors.NewDiskDetector())

	return p.Run(ctx, storage)
}
