package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
)

func TestTrialSyncEligibleFilter(t *testing.T) {
	filter := TrialSyncEligibleFilter()
	require.NotNil(t, filter)
	require.Len(t, filter.Conditions, 5)
	assert.Equal(t, "state", filter.Conditions[0].Field)
	assert.Equal(t, datamodel.AccountStateEnabled, filter.Conditions[0].Value)
	assert.Contains(t, filter.Conditions[1].Field, "trialMode")
}

func TestGetAccountsWithFilter(t *testing.T) {
	db, err := SetupTestDB()
	require.NoError(t, err)
	wrapper := gormwrapper.New(db)
	store := NewDataStoreRepository(wrapper)
	require.NoError(t, ClearInMemoryDB(store.db.GORM()))

	ctx := context.Background()

	enabled := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "list-acct-enabled"},
		Name:      "list_acct_enabled",
		State:     models.AccountStateEnabled,
	}
	disabled := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "list-acct-disabled"},
		Name:      "list_acct_disabled",
		State:     datamodel.AccountStateDisabled,
	}
	require.NoError(t, store.db.Create(enabled).Error())
	require.NoError(t, store.db.Create(disabled).Error())

	t.Run("nil filter returns all non-deleted accounts", func(t *testing.T) {
		got, err := store.GetAccountsWithFilter(ctx, nil, nil)
		require.NoError(t, err)
		require.Len(t, got, 2)
	})

	t.Run("state filter", func(t *testing.T) {
		filter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("state", "=", models.AccountStateEnabled),
		)
		got, err := store.GetAccountsWithFilter(ctx, filter, nil)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "list_acct_enabled", got[0].Name)
	})
}
