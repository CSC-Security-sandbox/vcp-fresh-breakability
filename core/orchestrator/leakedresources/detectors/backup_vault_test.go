package detectors

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

type mockCCFEBackupVaultLister struct {
	mock.Mock
}

func (m *mockCCFEBackupVaultLister) ListBackupVaults(ctx context.Context, projectID, location string) ([]string, error) {
	args := m.Called(ctx, projectID, location)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func backupVault(uuid, name, projectID, sourceRegion string) *datamodel.BackupVault {
	r := sourceRegion
	return &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{UUID: uuid},
		Name:             name,
		Account:          &datamodel.Account{Name: projectID},
		SourceRegionName: &r,
	}
}

func TestBackupVaultDetector_Name(t *testing.T) {
	ccfe := &mockCCFEBackupVaultLister{}
	d := NewBackupVaultDetector(ccfe)
	assert.Equal(t, "backup_vault", d.Name())
}

func TestBackupVaultDetector_Detect_GetMultipleFails(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(nil, errors.New("db error"))
	ccfe := &mockCCFEBackupVaultLister{}

	d := NewBackupVaultDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.Error(t, err)
	assert.Nil(t, records)
}

func TestBackupVaultDetector_Detect_NoVaults(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(nil, nil)
	ccfe := &mockCCFEBackupVaultLister{}

	d := NewBackupVaultDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertNotCalled(t, "ListBackupVaults")
}

func TestBackupVaultDetector_Detect_CCFEReturnsNil_SkipsGroup(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid-1", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	ccfe := &mockCCFEBackupVaultLister{}
	ccfe.On("ListBackupVaults", ctx, "proj1", "us-central1").Return(nil, nil)

	d := NewBackupVaultDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertExpectations(t)
}

func TestBackupVaultDetector_Detect_InCCFENotInVCP(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid-1", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	ccfe := &mockCCFEBackupVaultLister{}
	ccfe.On("ListBackupVaults", ctx, "proj1", "us-central1").Return([]string{"vault-a", "ccfe-only"}, nil)

	d := NewBackupVaultDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeBackupVault, records[0].ResourceType)
	assert.Equal(t, "ccfe-only", records[0].ResourceID)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "proj1", records[0].ProjectID)
	assert.Equal(t, "us-central1", records[0].Region)
}

func TestBackupVaultDetector_Detect_InVCPNotInCCFE(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid-1", "vault-a", "proj1", "us-central1"),
		backupVault("bv-uuid-2", "vault-b", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	ccfe := &mockCCFEBackupVaultLister{}
	ccfe.On("ListBackupVaults", ctx, "proj1", "us-central1").Return([]string{"vault-a"}, nil)

	d := NewBackupVaultDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, model.ResourceTypeBackupVault, records[0].ResourceType)
	assert.Equal(t, "bv-uuid-2", records[0].ResourceID)
	assert.Equal(t, "vault-b", records[0].ResourceName)
	assert.Equal(t, ReasonInVCPNotInCCFE, records[0].Reason)
	assert.Equal(t, "bv-uuid-2", records[0].Extra["uuid"])
}

func TestBackupVaultDetector_Detect_CCFEFails_SkipsGroup(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	ccfe := &mockCCFEBackupVaultLister{}
	ccfe.On("ListBackupVaults", ctx, "proj1", "us-central1").Return(nil, errors.New("ccfe error"))

	d := NewBackupVaultDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestBackupVaultDetector_Detect_NoLeaks(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	vaults := []*datamodel.BackupVault{
		backupVault("bv-uuid", "vault-a", "proj1", "us-central1"),
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	ccfe := &mockCCFEBackupVaultLister{}
	ccfe.On("ListBackupVaults", ctx, "proj1", "us-central1").Return([]string{"vault-a"}, nil)

	d := NewBackupVaultDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestBackupVaultDetector_Detect_FallbackBackupRegionWhenSourceEmpty(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	br := "us-east1"
	vaults := []*datamodel.BackupVault{
		{
			BaseModel:        datamodel.BaseModel{UUID: "bv-1"},
			Name:             "vault-x",
			Account:          &datamodel.Account{Name: "proj1"},
			BackupRegionName: &br,
		},
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	ccfe := &mockCCFEBackupVaultLister{}
	ccfe.On("ListBackupVaults", ctx, "proj1", "us-east1").Return([]string{"vault-x"}, nil)

	d := NewBackupVaultDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertExpectations(t)
}

func TestBackupVaultDetector_Detect_SkipsNoAccountOrRegion(t *testing.T) {
	ctx := context.Background()
	storage := database.NewMockStorage(t)
	r := "us-central1"
	vaults := []*datamodel.BackupVault{
		{BaseModel: datamodel.BaseModel{UUID: "1"}, Name: "v1", SourceRegionName: &r},                                       // no account
		{BaseModel: datamodel.BaseModel{UUID: "2"}, Name: "v2", Account: &datamodel.Account{Name: "p"}},                     // no region
		{BaseModel: datamodel.BaseModel{UUID: "3"}, Name: "", Account: &datamodel.Account{Name: "p"}, SourceRegionName: &r}, // empty name
	}
	storage.EXPECT().GetMultipleBackupVaults(ctx, mock.Anything).Return(vaults, nil)
	ccfe := &mockCCFEBackupVaultLister{}

	d := NewBackupVaultDetector(ccfe)
	records, err := d.Detect(ctx, storage)
	assert.NoError(t, err)
	assert.Empty(t, records)
	ccfe.AssertNotCalled(t, "ListBackupVaults")
}
