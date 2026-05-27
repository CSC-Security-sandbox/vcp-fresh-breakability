package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"gorm.io/gorm"
)

func TestGetAppConfig(t *testing.T) {
	t.Run("WhenKeyExists", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		cfg := &datamodel.AppConfig{
			BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
			Key:       "test_key",
			Value:     "test_value",
		}
		err = db.Create(cfg).Error
		assert.NoError(tt, err)

		result, err := store.GetAppConfig(tt.Context(), "test_key")
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test_key", result.Key)
		assert.Equal(tt, "test_value", result.Value)
	})

	t.Run("WhenKeyDoesNotExist", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		result, err := store.GetAppConfig(tt.Context(), "nonexistent_key")
		assert.Error(tt, err)
		assert.Nil(tt, result)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(tt, gorm.ErrRecordNotFound, customErr.OriginalErr)
		} else {
			tt.Fatalf("Expected a CustomError with RecordNotFound, got: %v", err)
		}
	})
}

func TestUpsertAppConfig(t *testing.T) {
	t.Run("WhenKeyDoesNotExist_CreatesNew", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		err = store.UpsertAppConfig(tt.Context(), "new_key", "new_value")
		assert.NoError(tt, err)

		result, err := store.GetAppConfig(tt.Context(), "new_key")
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "new_key", result.Key)
		assert.Equal(tt, "new_value", result.Value)
	})

	t.Run("WhenKeyExists_UpdatesValue", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		err = store.UpsertAppConfig(tt.Context(), "update_key", "original_value")
		assert.NoError(tt, err)

		err = store.UpsertAppConfig(tt.Context(), "update_key", "updated_value")
		assert.NoError(tt, err)

		result, err := store.GetAppConfig(tt.Context(), "update_key")
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "updated_value", result.Value)
	})

	t.Run("WhenMultipleKeysExist_UpdatesCorrectOne", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		err = store.UpsertAppConfig(tt.Context(), "key_a", "value_a")
		assert.NoError(tt, err)
		err = store.UpsertAppConfig(tt.Context(), "key_b", "value_b")
		assert.NoError(tt, err)

		err = store.UpsertAppConfig(tt.Context(), "key_a", "value_a_updated")
		assert.NoError(tt, err)

		resultA, err := store.GetAppConfig(tt.Context(), "key_a")
		assert.NoError(tt, err)
		assert.Equal(tt, "value_a_updated", resultA.Value)

		resultB, err := store.GetAppConfig(tt.Context(), "key_b")
		assert.NoError(tt, err)
		assert.Equal(tt, "value_b", resultB.Value)
	})

	// harvest_template_sha is the app_config key workflows.HarvestTemplateSHAAppConfigKey uses to detect template upgrades (VSCP-5327).
	t.Run("HarvestTemplateSHAKey_RoundTrip", func(tt *testing.T) {
		db, err := SetupTestDB()
		if err != nil {
			tt.Fatalf("Failed to set up test database: %v", err)
		}
		wrapper := gormwrapper.New(db)
		store := NewDataStoreRepository(wrapper)

		err = ClearInMemoryDB(store.db.GORM())
		if err != nil {
			tt.Fatalf("Failed to clean up test database: %v", err)
		}

		const harvestKey = "harvest_template_sha"
		templateSHA := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

		err = store.UpsertAppConfig(tt.Context(), harvestKey, templateSHA)
		assert.NoError(tt, err)

		got, err := store.GetAppConfig(tt.Context(), harvestKey)
		assert.NoError(tt, err)
		assert.Equal(tt, harvestKey, got.Key)
		assert.Equal(tt, templateSHA, got.Value)

		updated := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		err = store.UpsertAppConfig(tt.Context(), harvestKey, updated)
		assert.NoError(tt, err)

		got, err = store.GetAppConfig(tt.Context(), harvestKey)
		assert.NoError(tt, err)
		assert.Equal(tt, updated, got.Value)
	})
}
