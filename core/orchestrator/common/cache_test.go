package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
)

func TestInitAuthCaching(t *testing.T) {
	// Test that the cleanup task starts without errors
	assert.NotPanics(t, func() {
		initAuthCaching()
	}, "initAuthCaching should not panic")
}

func TestCleanupAuthCache(t *testing.T) {
	authCacheMap = map[string]*models.UserCache{
		"key1": {Time: time.Now().Add(-2 * authCacheExpiration), SecretID: "key1", Password: "pass1"},
		"key2": {Time: time.Now(), SecretID: "key2", Password: "pass2"},
	}
	cleanupAuthCache()

	_, exists1 := authCacheMap["key1"]
	_, exists2 := authCacheMap["key2"]

	assert.False(t, exists1, "key1 should be removed from the cache")
	assert.True(t, exists2, "key2 should still exist in the cache")
}

func TestGetAuthCache(t *testing.T) {
	authCacheMap = map[string]*models.UserCache{
		"key1": {Time: time.Now(), SecretID: "key1", Password: "pass1"},
		"key3": {Time: time.Now(), SecretID: "key3", Password: "pass1"},
	}

	authCache, exists := _getAuthCache("key1")
	assert.True(t, exists, "key1 should exist in the cache")
	assert.Equal(t, "pass1", authCache.Password, "Password should match")

	authCache2, exists := _getAuthCache("key3")
	assert.True(t, exists, "key3 should exist in the cache")
	assert.Equal(t, "pass1", authCache2.Password, "Password should match")

	_, exists = _getAuthCache("key2")
	assert.False(t, exists, "key2 should not exist in the cache")
}

func TestAddToAuthCache(t *testing.T) {
	authCacheMap = make(map[string]*models.UserCache)
	_addToAuthCache("key1", "pass1")

	authCache, exists := authCacheMap["key1"]
	assert.True(t, exists, "key1 should be added to the cache")
	assert.Equal(t, "pass1", authCache.Password, "Password should match")
}
func Test_removeFromCache(t *testing.T) {
	authCacheMap = make(map[string]*models.UserCache)
	key := "test-key"
	authCacheMap[key] = &models.UserCache{Time: time.Now(), SecretID: key, Password: "pass"}

	// Should remove existing key and return true
	removed := _removeFromCache(key)
	if !removed {
		t.Errorf("Expected true when removing existing key")
	}
	if _, exists := authCacheMap[key]; exists {
		t.Errorf("Key should be removed from cache")
	}

	// Should return false when key does not exist
	removed = _removeFromCache("non-existent-key")
	if removed {
		t.Errorf("Expected false when removing non-existent key")
	}
}
