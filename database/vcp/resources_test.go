package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataStoreRepository_CreatePendingResourceDeletion_EmptyResourceType(t *testing.T) {
	repo := &DataStoreRepository{}
	ctx := context.Background()

	result, err := repo.CreatePendingResourceDeletion(ctx, "", "test-bucket", "test error", "test-account", 123)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "An internal error occurred.")
}

func TestDataStoreRepository_CreatePendingResourceDeletion_EmptyResourceName(t *testing.T) {
	repo := &DataStoreRepository{}
	ctx := context.Background()

	result, err := repo.CreatePendingResourceDeletion(ctx, "bucket", "", "test error", "test-account", 123)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "An internal error occurred.")
}

func TestDataStoreRepository_ListPendingResourceDeletions_ZeroLimit(t *testing.T) {
	// Skip this test as it requires database initialization
	t.Skip("Skipping test that requires database initialization")
}

func TestDataStoreRepository_GetResourcesCount(t *testing.T) {
	// Skip this test as it requires database initialization
	t.Skip("Skipping test that requires database initialization")
}
