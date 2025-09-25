package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
)

func TestInitAuthDataCaching(t *testing.T) {
	assert.NotPanics(t, func() {
		InitializeAuthDataCaching()
	}, "InitializeAuthDataCaching should not panic")
}

func TestCleanupAuthDataCache(t *testing.T) {
	authDataCacheMap = map[string]*AuthDataCache{
		"pool1": {
			Time:   time.Now().Add(-2 * authDataExpiration),
			PoolID: "pool1",
			AuthData: &models.AuthData{
				PoolID:   "pool1",
				AuthType: models.USERNAME_PWD,
			},
		},
		"pool2": {
			Time:   time.Now(),
			PoolID: "pool2",
			AuthData: &models.AuthData{
				PoolID:   "pool2",
				AuthType: models.USER_CERTIFICATE,
			},
		},
	}

	CleanupAuthDataCache()

	_, exists1 := authDataCacheMap["pool1"]
	_, exists2 := authDataCacheMap["pool2"]

	assert.False(t, exists1, "pool1 should be removed from the cache (expired)")
	assert.True(t, exists2, "pool2 should still exist in the cache (not expired)")
}

func TestGetFromAuthDataCache(t *testing.T) {
	authDataCacheMap = map[string]*AuthDataCache{
		"pool1": {
			Time:   time.Now(),
			PoolID: "pool1",
			AuthData: &models.AuthData{
				PoolID:   "pool1",
				AuthType: models.USERNAME_PWD,
				Username: "user1",
				Password: "pass1",
			},
		},
		"pool2": {
			Time:   time.Now(),
			PoolID: "pool2",
			AuthData: &models.AuthData{
				PoolID:   "pool2",
				AuthType: models.USER_CERTIFICATE,
				Username: "user2",
			},
		},
	}

	authData, exists := GetFromAuthDataCache("pool1")
	assert.True(t, exists, "pool1 should exist in the cache")
	assert.Equal(t, "user1", authData.Username, "Username should match")
	assert.Equal(t, "pass1", authData.Password, "Password should match")
	assert.Equal(t, models.USERNAME_PWD, authData.AuthType, "AuthType should match")

	authData2, exists := GetFromAuthDataCache("pool2")
	assert.True(t, exists, "pool2 should exist in the cache")
	assert.Equal(t, "user2", authData2.Username, "Username should match")
	assert.Equal(t, models.USER_CERTIFICATE, authData2.AuthType, "AuthType should match")

	_, exists = GetFromAuthDataCache("pool3")
	assert.False(t, exists, "pool3 should not exist in the cache")
}

func TestAddToAuthDataCache(t *testing.T) {
	authDataCacheMap = make(map[string]*AuthDataCache)

	authData := &models.AuthData{
		PoolID:   "pool1",
		AuthType: models.USERNAME_PWD,
		Username: "user1",
		Password: "pass1",
		SecretID: "secret1",
	}

	AddToAuthDataCache("pool1", authData)

	cache, exists := authDataCacheMap["pool1"]
	assert.True(t, exists, "pool1 should be added to the cache")
	assert.Equal(t, "pool1", cache.PoolID, "PoolID should match")
	assert.Equal(t, authData, cache.AuthData, "AuthData should match")
	assert.True(t, time.Since(cache.Time) < time.Second, "Time should be recent")
}

func TestUpdateAuthDataInCache(t *testing.T) {
	authDataCacheMap = map[string]*AuthDataCache{
		"pool1": {
			Time:   time.Now().Add(-time.Hour),
			PoolID: "pool1",
			AuthData: &models.AuthData{
				PoolID:   "pool1",
				AuthType: models.USERNAME_PWD,
				Username: "olduser",
				Password: "oldpass",
			},
		},
	}

	newAuthData := &models.AuthData{
		PoolID:        "pool1",
		AuthType:      models.USER_CERTIFICATE,
		Username:      "newuser",
		Password:      "newpass",
		CertificateID: "cert1",
	}

	UpdateAuthDataInCache("pool1", newAuthData)

	cache, exists := authDataCacheMap["pool1"]
	assert.True(t, exists, "pool1 should still exist in the cache")
	assert.Equal(t, newAuthData, cache.AuthData, "AuthData should be updated")
	assert.True(t, time.Since(cache.Time) < time.Second, "Time should be updated to recent")

	UpdateAuthDataInCache("pool2", newAuthData)
	_, exists = authDataCacheMap["pool2"]
	assert.False(t, exists, "pool2 should not be added when updating non-existing key")
}

func TestRemoveFromAuthDataCache(t *testing.T) {
	authDataCacheMap = make(map[string]*AuthDataCache)
	key := "test-pool"
	authDataCacheMap[key] = &AuthDataCache{
		Time:   time.Now(),
		PoolID: key,
		AuthData: &models.AuthData{
			PoolID:   key,
			AuthType: models.USERNAME_PWD,
		},
	}

	removed := RemoveFromAuthDataCache(key)
	assert.True(t, removed, "Should return true when removing existing key")
	_, exists := authDataCacheMap[key]
	assert.False(t, exists, "Key should be removed from cache")

	removed = RemoveFromAuthDataCache("non-existent-key")
	assert.False(t, removed, "Should return false when removing non-existent key")
}

func TestGetAuthDataKeyFromContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), models.AuthDataKey, "test-key")
	key := GetAuthDataKeyFromContext(ctx)
	assert.Equal(t, "test-key", key, "Should return the correct key from context")

	ctx = context.WithValue(context.Background(), models.AuthDataKey, 123)
	key = GetAuthDataKeyFromContext(ctx)
	assert.Equal(t, "", key, "Should return empty string for invalid key type")

	ctx = context.Background()
	key = GetAuthDataKeyFromContext(ctx)
	assert.Equal(t, "", key, "Should return empty string for missing key")

	ctx = context.WithValue(context.Background(), models.RuleContextKey, "rule-key")
	key = GetAuthDataKeyFromContext(ctx)
	assert.Equal(t, "", key, "Should return empty string for different context key")
}

func TestConcurrentCacheOperations(t *testing.T) {
	authDataCacheMap = make(map[string]*AuthDataCache)
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			authData := &models.AuthData{
				PoolID:   "pool" + string(rune(i)),
				AuthType: models.USERNAME_PWD,
				Username: "user" + string(rune(i)),
			}
			AddToAuthDataCache("pool"+string(rune(i)), authData)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	assert.Len(t, authDataCacheMap, 10, "All entries should be added to cache")

	for i := 0; i < 10; i++ {
		go func(i int) {
			_, exists := GetFromAuthDataCache("pool" + string(rune(i)))
			assert.True(t, exists, "Pool should exist in cache")
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestCacheExpiration(t *testing.T) {
	now := time.Now()
	authDataCacheMap = map[string]*AuthDataCache{
		"expired": {
			Time:     now.Add(-authDataExpiration - time.Hour),
			PoolID:   "expired",
			AuthData: &models.AuthData{PoolID: "expired"},
		},
		"not_expired": {
			Time:     now.Add(-authDataExpiration + time.Hour),
			PoolID:   "not_expired",
			AuthData: &models.AuthData{PoolID: "not_expired"},
		},
		"recent": {
			Time:     now,
			PoolID:   "recent",
			AuthData: &models.AuthData{PoolID: "recent"},
		},
	}

	CleanupAuthDataCache()

	_, exists1 := authDataCacheMap["expired"]
	_, exists2 := authDataCacheMap["not_expired"]
	_, exists3 := authDataCacheMap["recent"]

	assert.False(t, exists1, "Expired entry should be removed")
	assert.True(t, exists2, "Not expired entry should remain")
	assert.True(t, exists3, "Recent entry should remain")
}

func TestAuthDataCacheStruct(t *testing.T) {
	authData := &models.AuthData{
		PoolID:   "test-pool",
		AuthType: models.USERNAME_PWD,
		Username: "testuser",
		Password: "testpass",
	}

	cache := &AuthDataCache{
		Time:     time.Now(),
		PoolID:   "test-pool",
		AuthData: authData,
	}

	assert.Equal(t, "test-pool", cache.PoolID, "PoolID should match")
	assert.Equal(t, authData, cache.AuthData, "AuthData should match")
	assert.True(t, time.Since(cache.Time) < time.Second, "Time should be recent")
}
