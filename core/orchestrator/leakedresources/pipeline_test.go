package leakedresources

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

// pinLocalRegionEmpty forces env.Region to "" for the lifetime of the test
// so the pool detector's enumerator short-circuits with an error instead
// of trying to call ListAccountsForTelemetry / GetRegionZonesWorkflow on
// a mock storage that wasn't expecting either. Without this, CI runs that
// happen to have LOCAL_REGION set would fail the unrelated TestRun cases.
func pinLocalRegionEmpty(t *testing.T) {
	t.Helper()
	orig := env.Region
	env.Region = ""
	t.Cleanup(func() { env.Region = orig })
}

type mockDetector struct {
	mock.Mock
}

func (m *mockDetector) Name() string {
	args := m.Called()
	return args.String(0)
}

func (m *mockDetector) Detect(ctx context.Context, storage database.Storage) ([]model.LeakRecord, error) {
	args := m.Called(ctx, storage)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.LeakRecord), args.Error(1)
}

type mockReporter struct {
	mock.Mock
}

func (m *mockReporter) Report(ctx context.Context, records []model.LeakRecord) error {
	args := m.Called(ctx, records)
	return args.Error(0)
}

func TestNewPipeline(t *testing.T) {
	p := NewPipeline()
	assert.NotNil(t, p)
	assert.Nil(t, p.detectors)
	assert.NotNil(t, p.reporter)
}

func TestPipeline_RegisterDetector(t *testing.T) {
	p := NewPipeline()
	d := &mockDetector{}
	d.On("Name").Return("test_detector")
	p.RegisterDetector(d)
	assert.Len(t, p.detectors, 1)
	p.RegisterDetector(nil)
	assert.Len(t, p.detectors, 1)
	p.RegisterDetector(&mockDetector{})
	assert.Len(t, p.detectors, 2)
}

func TestPipeline_SetReporter(t *testing.T) {
	p := NewPipeline()
	r := &mockReporter{}
	p.SetReporter(r)
	assert.Equal(t, r, p.reporter)
	p.SetReporter(nil)
	assert.Equal(t, r, p.reporter)
}

func TestPipeline_Run_NoDetectors(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	reporter := &mockReporter{}
	reporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	p := NewPipeline()
	p.SetReporter(reporter)
	err := p.Run(ctx, storage)
	assert.NoError(t, err)
	reporter.AssertExpectations(t)
}

func TestPipeline_Run_DetectorReturnsRecords(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	records := []model.LeakRecord{
		{ResourceType: model.ResourceTypePool, ResourceID: "p1", Reason: "in_vcp_not_in_ccfe"},
	}
	det := &mockDetector{}
	det.On("Name").Return("pool")
	det.On("Detect", mock.Anything, storage).Return(records, nil)

	reporter := &mockReporter{}
	reporter.On("Report", mock.Anything, records).Return(nil)

	p := NewPipeline()
	p.RegisterDetector(det)
	p.SetReporter(reporter)
	err := p.Run(ctx, storage)
	assert.NoError(t, err)
	det.AssertExpectations(t)
	reporter.AssertExpectations(t)
}

func TestPipeline_Run_DetectorFails_ContinuesAndReportsAggregated(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	okDet := &mockDetector{}
	okDet.On("Name").Return("ok")
	okDet.On("Detect", mock.Anything, storage).Return([]model.LeakRecord{{ResourceType: model.ResourceTypeVolume, ResourceID: "v1", Reason: "orphan"}}, nil)
	failDet := &mockDetector{}
	failDet.On("Name").Return("fail") // Pipeline calls Name() when logging detector failure
	failDet.On("Detect", mock.Anything, storage).Return(nil, errors.New("list failed"))

	reporter := &mockReporter{}
	reporter.On("Report", mock.Anything, []model.LeakRecord{{ResourceType: model.ResourceTypeVolume, ResourceID: "v1", Reason: "orphan"}}).Return(nil)

	p := NewPipeline()
	p.RegisterDetector(okDet)
	p.RegisterDetector(failDet)
	p.SetReporter(reporter)
	err := p.Run(ctx, storage)
	assert.NoError(t, err)
	okDet.AssertExpectations(t)
	failDet.AssertExpectations(t)
	reporter.AssertExpectations(t)
}

func TestPipeline_Run_ReporterFails_ReturnsError(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	det := &mockDetector{}
	det.On("Name").Return("empty")
	det.On("Detect", mock.Anything, storage).Return([]model.LeakRecord{}, nil)
	reporter := &mockReporter{}
	reporter.On("Report", mock.Anything, mock.Anything).Return(errors.New("report failed"))

	p := NewPipeline()
	p.RegisterDetector(det)
	p.SetReporter(reporter)
	err := p.Run(ctx, storage)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "report failed")
}

// TestRun exercises the package-level Run (default pipeline with all detectors registered).
// Uses mock storage returning empty data so no auth/CCFE/Temporal is required; ensures the
// default pipeline path is covered. With the IP/VM/Disk detectors all submitting their
// Compute API calls via Temporal, the empty-pools fast path means no workflow is ever submitted.
func TestRun(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	pinLocalRegionEmpty(t)

	// pool detector calls ListPoolsSelective first, then fails to enumerate
	// zones/accounts because pinLocalRegionEmpty forces LOCAL_REGION empty.
	storage.EXPECT().ListPoolsSelective(ctx, mock.Anything, mock.Anything).Return(nil, nil).Once()
	// internal_reserved_ip + volume detectors
	storage.EXPECT().ListPools(ctx, mock.Anything).Return(nil, nil).Times(2)
	// snapshot + volume detectors
	storage.EXPECT().ListVolumes(ctx, mock.Anything).Return(nil, nil).Times(2)
	// snapshot detector accountID→name map
	storage.EXPECT().GetAccounts(ctx, false, mock.Anything).Return(nil, nil).Once()
	// vm_orphan + disk_orphan detectors call ListAllTpProjects and exit on empty
	storage.EXPECT().ListAllTpProjects(ctx).Return(nil, nil).Times(2)
	storage.EXPECT().GetSnapshotsWithCondition(ctx, mock.Anything).Return(nil, nil).Once()
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(nil, nil).Once()

	err := Run(ctx, storage, nil)
	assert.NoError(t, err)
}
