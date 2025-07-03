package common

import (
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

var (
	userAuthCacheMutex sync.Mutex
	userAuthCacheMap   = map[string]*models.UserCache{} // map of secretID to password

	cacheCleanupInterval = time.Duration(env.GetInt("VSA_SECRET_CACHE_CLEANUP_INTERVAL_HOURS", 24)) * time.Hour
	authCacheExpiration  = time.Duration(env.GetInt("VSA_SECRET_AUTH_CACHE_EXPIRATION_HOURS", 24)) * time.Hour

	GetFromUserAuthCache    = _getFromUserAuthCache
	AddToUserAuthCache      = _addToUserAuthCache
	InitializeAuthCaching   = initAuthCaching
	RemoveFromUserAuthCache = _removeFromUserAuthCache
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
		cleanupUserAuthCache()
	}
}

func cleanupUserAuthCache() {
	userAuthCacheMutex.Lock()
	defer userAuthCacheMutex.Unlock()
	for apiKey, value := range userAuthCacheMap {
		if time.Since(value.Time) > authCacheExpiration {
			delete(userAuthCacheMap, apiKey)
		}
	}
}

func _getFromUserAuthCache(key string) (*models.UserCache, bool) {
	userAuthCacheMutex.Lock()
	defer userAuthCacheMutex.Unlock()
	authCache, exists := userAuthCacheMap[key]
	if !exists {
		return nil, false
	}
	return authCache, exists
}

func _addToUserAuthCache(key, value string) {
	authCache, exists := userAuthCacheMap[key]
	if !exists || authCache.Password == "" {
		userAuthCacheMutex.Lock()
		defer userAuthCacheMutex.Unlock()
		userAuthCacheMap[key] = &models.UserCache{Time: time.Now(), SecretID: key, Password: value}
	}
}

func _removeFromUserAuthCache(key string) bool {
	userAuthCacheMutex.Lock()
	defer userAuthCacheMutex.Unlock()
	_, exists := userAuthCacheMap[key]
	if !exists {
		return false
	}
	delete(userAuthCacheMap, key)
	return true
}
