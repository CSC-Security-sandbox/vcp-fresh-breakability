package common

import (
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

var (
	userAuthCacheMutex sync.RWMutex
	userAuthCacheMap   = map[string]*models.UserCache{} // map of secretID to password
	certAuthCacheMutex sync.RWMutex
	certAuthCacheMap   = map[string]*models.CertCache{} // map of certificateID to Certificate

	cacheCleanupInterval = time.Duration(env.GetInt("VSA_SECRET_CACHE_CLEANUP_INTERVAL_HOURS", 24)) * time.Hour
	authCacheExpiration  = time.Duration(env.GetInt("VSA_SECRET_AUTH_CACHE_EXPIRATION_HOURS", 24)) * time.Hour

	GetFromUserAuthCache    = _getFromUserAuthCache
	AddToUserAuthCache      = _addToUserAuthCache
	InitializeAuthCaching   = initAuthCaching
	RemoveFromUserAuthCache = _removeFromUserAuthCache

	GetCertAuthCache        = _getCertAuthCache
	AddToCertAuthCache      = _addToCertAuthCache
	RemoveFromCertAuthCache = _removeFromCertAuthCache
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
		cleanupCertAuthCache()
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
	userAuthCacheMutex.RLock()
	authCache, exists := userAuthCacheMap[key]
	userAuthCacheMutex.RUnlock()
	if !exists {
		return nil, false
	}
	return authCache, exists
}

func _addToUserAuthCache(key, value string) {
	userAuthCacheMutex.RLock()
	authCache, exists := userAuthCacheMap[key]
	userAuthCacheMutex.RUnlock()
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
func cleanupCertAuthCache() {
	certAuthCacheMutex.Lock()
	defer certAuthCacheMutex.Unlock()
	for apiKey, value := range certAuthCacheMap {
		if time.Since(value.Time) > authCacheExpiration {
			delete(certAuthCacheMap, apiKey)
		}
	}
}

func _getCertAuthCache(key string) (*models.CertCache, bool) {
	// TODO - Update this for supporting rotating certificates
	certAuthCacheMutex.RLock()
	authCache, exists := certAuthCacheMap[key]
	certAuthCacheMutex.RUnlock()
	if !exists {
		return nil, false
	}
	return authCache, exists
}

func _addToCertAuthCache(key string, value *models.Certificate) {
	certAuthCacheMutex.RLock()
	authCache, exists := certAuthCacheMap[key]
	certAuthCacheMutex.RUnlock()
	if !exists || authCache.Certificate == nil {
		certAuthCacheMutex.Lock()
		defer certAuthCacheMutex.Unlock()
		certAuthCacheMap[key] = &models.CertCache{Time: time.Now(), CertificateID: key, Certificate: value}
	}
}

func _removeFromCertAuthCache(key string) bool {
	certAuthCacheMutex.Lock()
	defer certAuthCacheMutex.Unlock()
	_, exists := certAuthCacheMap[key]
	if !exists {
		return false
	}
	delete(certAuthCacheMap, key)
	return true
}
