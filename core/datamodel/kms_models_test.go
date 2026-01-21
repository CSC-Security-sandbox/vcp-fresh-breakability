package datamodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServiceAccount_GetPrimaryKey(t *testing.T) {
	t.Run("ReturnsPrimaryKey_WhenExists", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", IsPrimary: false},
					{KeyID: "key-2", IsPrimary: true},
					{KeyID: "key-3", IsPrimary: false},
				},
			},
		}
		primaryKey := sa.GetPrimaryKey()
		assert.NotNil(t, primaryKey)
		assert.Equal(t, "key-2", primaryKey.KeyID)
		assert.True(t, primaryKey.IsPrimary)
	})

	t.Run("ReturnsNil_WhenNoPrimaryKey", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", IsPrimary: false},
					{KeyID: "key-2", IsPrimary: false},
				},
			},
		}
		primaryKey := sa.GetPrimaryKey()
		assert.Nil(t, primaryKey)
	})

	t.Run("ReturnsNil_WhenAttributesNil", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: nil,
		}
		primaryKey := sa.GetPrimaryKey()
		assert.Nil(t, primaryKey)
	})

	t.Run("ReturnsNil_WhenKeysEmpty", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{},
			},
		}
		primaryKey := sa.GetPrimaryKey()
		assert.Nil(t, primaryKey)
	})
}

func TestServiceAccount_GetKeyByID(t *testing.T) {
	t.Run("ReturnsKey_WhenExists", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", KeyData: "data-1"},
					{KeyID: "key-2", KeyData: "data-2"},
					{KeyID: "key-3", KeyData: "data-3"},
				},
			},
		}
		key := sa.GetKeyByID("key-2")
		assert.NotNil(t, key)
		assert.Equal(t, "key-2", key.KeyID)
		assert.Equal(t, "data-2", key.KeyData)
	})

	t.Run("ReturnsNil_WhenKeyNotFound", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", KeyData: "data-1"},
					{KeyID: "key-2", KeyData: "data-2"},
				},
			},
		}
		key := sa.GetKeyByID("nonexistent-key")
		assert.Nil(t, key)
	})

	t.Run("ReturnsNil_WhenAttributesNil", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: nil,
		}
		key := sa.GetKeyByID("key-1")
		assert.Nil(t, key)
	})

	t.Run("ReturnsNil_WhenKeysEmpty", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{},
			},
		}
		key := sa.GetKeyByID("key-1")
		assert.Nil(t, key)
	})
}

func TestServiceAccount_AddKey(t *testing.T) {
	t.Run("AddsKey_WhenAttributesExist", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1"},
				},
			},
		}
		newKey := ServiceAccountKey{
			KeyID:     "key-2",
			KeyData:   "data-2",
			IsPrimary: true,
			IsActive:  true,
			CreatedAt: time.Now(),
		}
		sa.AddKey(newKey)
		assert.Len(t, sa.ServiceAccountAttributes.Keys, 2)
		assert.Equal(t, "key-2", sa.ServiceAccountAttributes.Keys[1].KeyID)
		assert.Equal(t, "data-2", sa.ServiceAccountAttributes.Keys[1].KeyData)
		assert.True(t, sa.ServiceAccountAttributes.Keys[1].IsPrimary)
	})

	t.Run("InitializesAttributes_WhenNil", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: nil,
		}
		newKey := ServiceAccountKey{
			KeyID:   "key-1",
			KeyData: "data-1",
		}
		sa.AddKey(newKey)
		assert.NotNil(t, sa.ServiceAccountAttributes)
		assert.Len(t, sa.ServiceAccountAttributes.Keys, 1)
		assert.Equal(t, "key-1", sa.ServiceAccountAttributes.Keys[0].KeyID)
	})

	t.Run("AddsMultipleKeys", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{},
			},
		}
		sa.AddKey(ServiceAccountKey{KeyID: "key-1"})
		sa.AddKey(ServiceAccountKey{KeyID: "key-2"})
		sa.AddKey(ServiceAccountKey{KeyID: "key-3"})
		assert.Len(t, sa.ServiceAccountAttributes.Keys, 3)
	})
}

func TestServiceAccount_RemoveKey(t *testing.T) {
	t.Run("RemovesKey_WhenExists", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1"},
					{KeyID: "key-2"},
					{KeyID: "key-3"},
				},
			},
		}
		removed := sa.RemoveKey("key-2")
		assert.True(t, removed)
		assert.Len(t, sa.ServiceAccountAttributes.Keys, 2)
		assert.Equal(t, "key-1", sa.ServiceAccountAttributes.Keys[0].KeyID)
		assert.Equal(t, "key-3", sa.ServiceAccountAttributes.Keys[1].KeyID)
	})

	t.Run("RemovesFirstKey", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1"},
					{KeyID: "key-2"},
				},
			},
		}
		removed := sa.RemoveKey("key-1")
		assert.True(t, removed)
		assert.Len(t, sa.ServiceAccountAttributes.Keys, 1)
		assert.Equal(t, "key-2", sa.ServiceAccountAttributes.Keys[0].KeyID)
	})

	t.Run("RemovesLastKey", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1"},
					{KeyID: "key-2"},
				},
			},
		}
		removed := sa.RemoveKey("key-2")
		assert.True(t, removed)
		assert.Len(t, sa.ServiceAccountAttributes.Keys, 1)
		assert.Equal(t, "key-1", sa.ServiceAccountAttributes.Keys[0].KeyID)
	})

	t.Run("ReturnsFalse_WhenKeyNotFound", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1"},
					{KeyID: "key-2"},
				},
			},
		}
		removed := sa.RemoveKey("nonexistent-key")
		assert.False(t, removed)
		assert.Len(t, sa.ServiceAccountAttributes.Keys, 2)
	})

	t.Run("ReturnsFalse_WhenAttributesNil", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: nil,
		}
		removed := sa.RemoveKey("key-1")
		assert.False(t, removed)
	})

	t.Run("ReturnsFalse_WhenKeysEmpty", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{},
			},
		}
		removed := sa.RemoveKey("key-1")
		assert.False(t, removed)
	})
}

func TestServiceAccount_SetPrimaryKey(t *testing.T) {
	t.Run("SetsPrimaryKey_WhenKeyExists", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", IsPrimary: true},
					{KeyID: "key-2", IsPrimary: false},
					{KeyID: "key-3", IsPrimary: false},
				},
			},
		}
		found := sa.SetPrimaryKey("key-2")
		assert.True(t, found)
		assert.False(t, sa.ServiceAccountAttributes.Keys[0].IsPrimary)
		assert.True(t, sa.ServiceAccountAttributes.Keys[1].IsPrimary)
		assert.False(t, sa.ServiceAccountAttributes.Keys[2].IsPrimary)
	})

	t.Run("SetsPrimaryKey_WhenAlreadyPrimary", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", IsPrimary: true},
					{KeyID: "key-2", IsPrimary: false},
				},
			},
		}
		found := sa.SetPrimaryKey("key-1")
		assert.True(t, found)
		assert.True(t, sa.ServiceAccountAttributes.Keys[0].IsPrimary)
		assert.False(t, sa.ServiceAccountAttributes.Keys[1].IsPrimary)
	})

	t.Run("ReturnsFalse_WhenKeyNotFound", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", IsPrimary: true},
					{KeyID: "key-2", IsPrimary: false},
				},
			},
		}
		found := sa.SetPrimaryKey("nonexistent-key")
		assert.False(t, found)
		// Keys should remain unchanged
		assert.True(t, sa.ServiceAccountAttributes.Keys[0].IsPrimary)
		assert.False(t, sa.ServiceAccountAttributes.Keys[1].IsPrimary)
	})

	t.Run("ReturnsFalse_WhenAttributesNil", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: nil,
		}
		found := sa.SetPrimaryKey("key-1")
		assert.False(t, found)
	})

	t.Run("ReturnsFalse_WhenKeysEmpty", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{},
			},
		}
		found := sa.SetPrimaryKey("key-1")
		assert.False(t, found)
	})
}

func TestServiceAccount_GetAllActiveKeys(t *testing.T) {
	t.Run("ReturnsAllActiveKeys", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", IsActive: true},
					{KeyID: "key-2", IsActive: false},
					{KeyID: "key-3", IsActive: true},
					{KeyID: "key-4", IsActive: true},
				},
			},
		}
		activeKeys := sa.GetAllActiveKeys()
		assert.Len(t, activeKeys, 3)
		assert.Equal(t, "key-1", activeKeys[0].KeyID)
		assert.Equal(t, "key-3", activeKeys[1].KeyID)
		assert.Equal(t, "key-4", activeKeys[2].KeyID)
	})

	t.Run("ReturnsEmpty_WhenNoActiveKeys", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", IsActive: false},
					{KeyID: "key-2", IsActive: false},
				},
			},
		}
		activeKeys := sa.GetAllActiveKeys()
		assert.Len(t, activeKeys, 0)
	})

	t.Run("ReturnsEmpty_WhenAttributesNil", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: nil,
		}
		activeKeys := sa.GetAllActiveKeys()
		assert.Len(t, activeKeys, 0)
		assert.NotNil(t, activeKeys)
	})

	t.Run("ReturnsEmpty_WhenKeysEmpty", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{},
			},
		}
		activeKeys := sa.GetAllActiveKeys()
		assert.Len(t, activeKeys, 0)
	})

	t.Run("ReturnsAllKeys_WhenAllActive", func(t *testing.T) {
		sa := &ServiceAccount{
			ServiceAccountAttributes: &ServiceAccountAttributes{
				Keys: []ServiceAccountKey{
					{KeyID: "key-1", IsActive: true},
					{KeyID: "key-2", IsActive: true},
				},
			},
		}
		activeKeys := sa.GetAllActiveKeys()
		assert.Len(t, activeKeys, 2)
	})
}
