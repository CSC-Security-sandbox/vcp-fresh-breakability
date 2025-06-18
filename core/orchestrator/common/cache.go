package common

import (
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

var (
	authCacheMutex       sync.Mutex
	authCacheMap         = map[string]*models.UserCache{} // map of secretID to password
	cacheCleanupInterval = time.Duration(env.GetInt("VSA_SECRET_CACHE_CLEANUP_INTERVAL_MINUTES", 5)) * time.Hour
	authCacheExpiration  = time.Duration(env.GetInt("VSA_SECRET_AUTH_CACHE_EXPIRATION_MINUTES", 5)) * time.Hour

	GetAuthCache          = _getAuthCache
	AddToAuthCache        = _addToAuthCache
	InitializeAuthCaching = initAuthCaching
	RemoveFromCache       = _removeFromCache
)

func init() {
	InitializeAuthCaching()
}

func initAuthCaching() {
	go cleanupCachingTask()
}

func cleanupCachingTask() {
	for {
		time.Sleep(cacheCleanupInterval)
		cleanupAuthCache()
	}
}

func cleanupAuthCache() {
	authCacheMutex.Lock()
	defer authCacheMutex.Unlock()
	for apiKey, value := range authCacheMap {
		if time.Since(value.Time) > authCacheExpiration {
			delete(authCacheMap, apiKey)
		}
	}
}

func _getAuthCache(key string) (*models.UserCache, bool) {
	authCacheMutex.Lock()
	defer authCacheMutex.Unlock()
	authCache, exists := authCacheMap[key]
	if !exists {
		return nil, false
	}
	return authCache, exists
}

func _addToAuthCache(key, value string) {
	authCache, exists := authCacheMap[key]
	if !exists || authCache.Password == "" {
		authCacheMutex.Lock()
		defer authCacheMutex.Unlock()
		authCacheMap[key] = &models.UserCache{Time: time.Now(), SecretID: key, Password: value}
	}
}

func _removeFromCache(key string) bool {
	authCacheMutex.Lock()
	defer authCacheMutex.Unlock()
	_, exists := authCacheMap[key]
	if !exists {
		return false
	}
	delete(authCacheMap, key)
	return true
}
