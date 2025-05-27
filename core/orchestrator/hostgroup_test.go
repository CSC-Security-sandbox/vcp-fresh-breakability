package orchestrator

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"golang.org/x/net/context"
)

func TestGetHostGroup(t *testing.T) {
	t.Run("WhenHostGroupDoesNotExist", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		hg, err := orch.GetHostGroup(ctx, "non-existent-uuid", account.Name)
		assert.EqualError(tt, err, "host group not found")
		assert.Nil(tt, hg, "Expected nil volume")
	})
	t.Run("WhenHostGroupExist", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "test-hg-uuid"},
			Name:      "test_hg",
			AccountID: account.ID,
		}
		err = store.DB().Create(hg).Error
		assert.NoError(tt, err, "Failed to create hg")

		hgResp, err := orch.GetHostGroup(ctx, hg.UUID, account.Name)
		assert.NoError(tt, err, "Failed to get hg")
		assert.NotNil(tt, hgResp)
	})
}

func TestGetMultipleHostGroups(t *testing.T) {
	t.Run("WhenGetMultipleHostGroupsAccountNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		hgResp, err := orch.GetMultipleHostGroups(ctx, "account", []string{})
		assert.EqualError(tt, err, "account not found")
		assert.Len(tt, hgResp, 0)
	})
	t.Run("WhenGetMultipleHostGroupsSuccess", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		hg1 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "test-hg-uuid1"},
			Name:      "test_hg",
			AccountID: account.ID,
		}
		err = store.DB().Create(hg1).Error
		assert.NoError(tt, err, "Failed to create hg")

		hg2 := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{UUID: "test-hg-uuid2"},
			Name:      "test_hg",
			AccountID: account.ID,
		}
		err = store.DB().Create(hg2).Error
		assert.NoError(tt, err, "Failed to create hg")

		hgResp, err := orch.GetMultipleHostGroups(ctx, account.Name, []string{hg1.UUID, hg2.UUID})
		assert.Nil(tt, err, "some error")
		assert.Len(tt, hgResp, 2)
	})
	t.Run("WhenGetMultipleHostGroupsNoHG", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		hgResp, err := orch.GetMultipleHostGroups(ctx, account.Name, []string{"a", "b"})
		assert.Nil(tt, err, "some error")
		assert.Len(tt, hgResp, 0)
	})
}

func TestDeleteHostGroups(t *testing.T) {
	t.Run("WhenDeleteHostGroupsNotFound", func(tt *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

		mockLogger := log.NewLogger()
		store, err := database.NewTestStorage(mockLogger)
		if err != nil {
			tt.Fatalf("Failed to create test storage: %v", err)
		}

		// Clear the in-memory database
		err = database.ClearInMemoryDB(store.DB())
		if err != nil {
			t.Fatalf("Failed to clean up test storage: %v", err)
		}

		orch := Orchestrator{
			storage: store,
		}

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test_account",
		}
		err = store.DB().Create(account).Error
		if err != nil {
			tt.Fatalf("Failed to create account: %v", err)
		}

		defer func() {
			deleteHostGroup = _deleteHostGroup
		}()

		deleteHostGroup = func(ctx context.Context, storage database.Storage, hostGroupUUID string, accountID string) (*models.HostGroup, error) {
			return nil, customerrors.NewNotFoundErr("host group", nil)
		}
		_, err = orch.DeleteHostGroup(ctx, account.Name, "non-existent-uuid")
		assert.EqualError(tt, err, "host group not found")
	})
}
