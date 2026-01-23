package cache

import (
	"context"
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

type AuthDataCache struct {
	Time     time.Time
	PoolID   string
	AuthData *models.AuthData
}

// CacheEntryStatus contains non-sensitive cache entry metadata
type CacheEntryStatus struct {
	CacheKey  string
	CachedAt  time.Time
	ExpiresAt time.Time
}

var (
	CleanupAuthDataCache = cleanupAuthDataCache
	authDataCacheMutex   sync.RWMutex
	authDataCacheMap     = map[string]*AuthDataCache{}

	cacheCleanupInterval = time.Duration(env.GetInt("AUTH_DATA_CACHE_CLEANUP_INTERVAL_MINUTES", 10080)) * time.Minute
	authDataExpiration   = time.Duration(env.GetInt("AUTH_DATA_CACHE_EXPIRATION_MINUTES", 10080)) * time.Minute

	GetFromAuthDataCache      = _getFromAuthDataCache
	AddToAuthDataCache        = _addToAuthDataCache
	UpdateAuthDataInCache     = _updateAuthDataInCache
	InitializeAuthDataCaching = initAuthDataCaching
	RemoveFromAuthDataCache   = _removeFromAuthDataCache
)

func init() {
	InitializeAuthDataCaching()
}

func initAuthDataCaching() {
	go cleanupCachingTask()
}

func cleanupCachingTask() {
	for {
		time.Sleep(cacheCleanupInterval)
		cleanupAuthDataCache()
	}
}

func cleanupAuthDataCache() {
	authDataCacheMutex.Lock()
	defer authDataCacheMutex.Unlock()
	for poolID, value := range authDataCacheMap {
		if time.Since(value.Time) > authDataExpiration {
			delete(authDataCacheMap, poolID)
		}
	}
}

func _getFromAuthDataCache(key string) (*models.AuthData, bool) {
	authDataCacheMutex.RLock()
	defer authDataCacheMutex.RUnlock()
	authCache, exists := authDataCacheMap[key]
	if !exists {
		return nil, false
	}
	return authCache.AuthData, exists
}

func _addToAuthDataCache(key string, authData *models.AuthData) {
	authDataCacheMutex.Lock()
	defer authDataCacheMutex.Unlock()
	authDataCacheMap[key] = &AuthDataCache{
		Time:     time.Now(),
		PoolID:   key,
		AuthData: authData,
	}
}

func _updateAuthDataInCache(key string, authData *models.AuthData) {
	authDataCacheMutex.Lock()
	defer authDataCacheMutex.Unlock()
	if authCache, exists := authDataCacheMap[key]; exists {
		authCache.AuthData = authData
		authCache.Time = time.Now() // Update the timestamp
	}
}

func _removeFromAuthDataCache(key string) bool {
	authDataCacheMutex.Lock()
	defer authDataCacheMutex.Unlock()
	_, exists := authDataCacheMap[key]
	if !exists {
		return false
	}
	delete(authDataCacheMap, key)
	return true
}

func GetAuthDataKeyFromContext(ctx context.Context) string {
	if cacheKey, ok := ctx.Value(models.AuthDataKey).(string); ok {
		return cacheKey
	}
	return ""
}

// GetAuthDataCacheStatus returns cache status information without sensitive data
func GetAuthDataCacheStatus() []CacheEntryStatus {
	authDataCacheMutex.RLock()
	defer authDataCacheMutex.RUnlock()

	entries := make([]CacheEntryStatus, 0, len(authDataCacheMap))
	for key, value := range authDataCacheMap {
		entries = append(entries, CacheEntryStatus{
			CacheKey:  key,
			CachedAt:  value.Time,
			ExpiresAt: value.Time.Add(authDataExpiration),
		})
	}
	return entries
}
