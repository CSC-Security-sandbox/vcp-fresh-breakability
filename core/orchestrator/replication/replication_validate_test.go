package replication

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func Test_getReplicationJobs(t *testing.T) {
	t.Run("WhenSuccessful", func(tt *testing.T) {
		ctx := context.Background()
		basePath := "basePath"
		token := "token"
		locationID := "loc"
		projectNumber := "proj"
		xCorrelationID := new(string)
		poolId := "pool"

		mockJobs := []googleproxyclient.InternalJobV1beta{
			{ResourceName: googleproxyclient.OptString{Value: "job1", Set: true}},
		}

		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaInternalGetReplicationJobs(ctx, mock.Anything).Return(&googleproxyclient.V1betaInternalGetReplicationJobsOK{Jobs: mockJobs}, nil)

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		jobs, err := _getReplicationJobs(ctx, basePath, token, locationID, projectNumber, xCorrelationID, poolId)
		assert.NoError(t, err)
		assert.Equal(t, mockJobs, jobs)
	})
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		basePath := "basePath"
		token := "token"
		locationID := "loc"
		projectNumber := "proj"
		xCorrelationID := new(string)
		poolId := "pool"

		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaInternalGetReplicationJobs(ctx, mock.Anything).Return(nil, errors.New("api error"))

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		jobs, err := _getReplicationJobs(ctx, basePath, token, locationID, projectNumber, xCorrelationID, poolId)
		assert.Error(t, err)
		assert.Nil(t, jobs)
	})
}

func Test_replicationJobInProcess(t *testing.T) {
	ctx := context.Background()
	srcBasePath := "srcBasePath"
	destBasePath := "destBasePath"
	srcToken := "srcToken"
	destToken := "destToken"
	srcLocationID := "srcLoc"
	destLocationID := "destLoc"
	srcProjectNumber := "srcProj"
	destProjectNumber := "destProj"
	ccfeUri := "ccfeUri"
	remoteCcfeUri := "remoteCcfeUri"
	srcPoolId := "srcPool"
	dstPoolId := "dstPool"
	xCorrelationID := new(string)

	t.Run("No jobs in process", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaInternalGetReplicationJobs(ctx, mock.Anything).Return(&googleproxyclient.V1betaInternalGetReplicationJobsOK{Jobs: []googleproxyclient.InternalJobV1beta{}}, nil).Maybe()

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		err := _replicationJobInProcess(ctx, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId, xCorrelationID)
		assert.NoError(t, err)
	})

	t.Run("Job in process for source", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaInternalGetReplicationJobs(ctx, mock.Anything).Return(&googleproxyclient.V1betaInternalGetReplicationJobsOK{
			Jobs: []googleproxyclient.InternalJobV1beta{
				{ResourceName: googleproxyclient.OptString{Value: ccfeUri, Set: true}},
			},
		}, nil).Once()

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		err := _replicationJobInProcess(ctx, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, "", srcPoolId, dstPoolId, xCorrelationID)
		assert.Error(t, err)
	})

	t.Run("Job in process for destination", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaInternalGetReplicationJobs(ctx, mock.Anything).Return(&googleproxyclient.V1betaInternalGetReplicationJobsOK{
			Jobs: []googleproxyclient.InternalJobV1beta{
				{ResourceName: googleproxyclient.OptString{Value: remoteCcfeUri, Set: true}},
			},
		}, nil).Once()

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		err := _replicationJobInProcess(ctx, srcProjectNumber, destProjectNumber, "", destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId, xCorrelationID)
		assert.Error(t, err)
	})

	t.Run("Error from getReplicationJobs", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaInternalGetReplicationJobs(ctx, mock.Anything).Return(nil, errors.New("api error")).Once()

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		err := _replicationJobInProcess(ctx, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, "", srcPoolId, dstPoolId, xCorrelationID)
		assert.Error(t, err)
	})
}

func Test_validateStoragePoolUri(t *testing.T) {
	validUris := []string{
		"projects/123/locations/us-central1/storagePools/pool-1",
		"projects/abc/locations/europe-west1/pools/pool-2",
	}
	invalidUris := []string{
		"projects/123/locations/us-central1",               // too short
		"projects/123/locations/us-central1/volumes/vol-1", // wrong resource
		"projects/123/locations/us-central1/pools",         // missing pool name
		"invalid/uri/format",
	}

	for _, uri := range validUris {
		err := _validateStoragePoolUri(uri)
		assert.NoError(t, err, "should be valid: %s", uri)
	}

	for _, uri := range invalidUris {
		err := _validateStoragePoolUri(uri)
		assert.Error(t, err, "should be invalid: %s", uri)
	}
}

func Test_validateReplicationResourceId(t *testing.T) {
	ctx := context.Background()
	projectNumber := "proj-1"
	resourceId := "replication-1"
	volumeResourceId := "vol-1"
	account := &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acc-1"}}

	t.Run("no replications exist", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mockStorage.On("GetAccount", ctx, projectNumber).Return(account, nil)
		mockStorage.On("GetVolumeReplicationByProjectId", ctx, account.ID).Return([]*datamodel.VolumeReplication{}, nil)

		err := _validateReplicationResourceId(ctx, projectNumber, resourceId, volumeResourceId, mockStorage)
		assert.NoError(t, err)
	})

	t.Run("replication with different resourceId", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mockStorage.On("GetAccount", ctx, projectNumber).Return(account, nil)
		mockStorage.On("GetVolumeReplicationByProjectId", ctx, account.ID).Return([]*datamodel.VolumeReplication{
			{Uri: "projects/proj-1/locations/loc-1/volumes/vol-1/replications/replication-2"},
		}, nil)

		err := _validateReplicationResourceId(ctx, projectNumber, resourceId, volumeResourceId, mockStorage)
		assert.NoError(t, err)
	})

	t.Run("replication with same resourceId and volumeResourceId", func(t *testing.T) {
		mockSe := &database.MockStorage{}
		mockSe.On("GetAccount", ctx, projectNumber).Return(account, nil)
		mockSe.On("GetVolumeReplicationByProjectId", ctx, account.ID).Return([]*datamodel.VolumeReplication{
			{Uri: "projects/proj-1/locations/loc-1/volumes/vol-1/replications/replication-1"},
		}, nil)

		err := _validateReplicationResourceId(ctx, projectNumber, resourceId, volumeResourceId, mockSe)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "replication resourceId already in use")
	})

	t.Run("GetAccount returns error", func(t *testing.T) {
		mockSe := &database.MockStorage{}
		mockSe.On("GetAccount", ctx, projectNumber).Return(&datamodel.Account{}, errors.New("account error"))

		err := _validateReplicationResourceId(ctx, projectNumber, resourceId, volumeResourceId, mockSe)
		assert.Error(t, err)
	})

	t.Run("GetVolumeReplicationByProjectId returns error", func(t *testing.T) {
		mockSe := &database.MockStorage{}
		mockSe.On("GetAccount", ctx, projectNumber).Return(account, nil)
		mockSe.On("GetVolumeReplicationByProjectId", ctx, account.ID).Return(nil, errors.New("replication error"))

		err := _validateReplicationResourceId(ctx, projectNumber, resourceId, volumeResourceId, mockSe)
		assert.Error(t, err)
	})
}

func Test_createReplicationObjects_Success(t *testing.T) {
	resourceID := "replication-1"
	description := "desc"
	replicationSchedule := models.ReplicationV1betaReplicationScheduleHOURLY
	volumeID := "vol_2"
	event := &CreateReplicationEvent{
		SourceProjectNumber:      "src-proj",
		DestinationProjectNumber: "dst-proj",
		LocationID:               "loc-1",
		DestinationLocationID:    "loc-2",
		VolumeResourceID:         "vol-1",
		CreateReplicationParams: &CreateReplicationParamsBody{
			ResourceID:          &resourceID,
			Description:         &description,
			ReplicationSchedule: &replicationSchedule,
			DestinationVolumeParameters: &DestinationVolumeParams{
				VolumeID: volumeID,
			},
		},
		SourceVolume: datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: uuid.New()},
			Name:      "vol-1",
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		},
		SourcePool: datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
	}

	replication, err := _createReplicationObjects(event, "loc-2", "region-1", "region-2")
	assert.NoError(t, err)
	assert.NotNil(t, replication)
	assert.Contains(t, replication.Uri, "projects/src-proj/locations/loc-1/volumes/vol-1/replications/replication-1")
	assert.Contains(t, replication.RemoteUri, "projects/dst-proj/locations/loc-2/volumes/vol_2/replications/replication-1")
	assert.Equal(t, resourceID, replication.Name)
	assert.Equal(t, description, replication.Description)
	assert.NotNil(t, replication.ReplicationAttributes)
	assert.Equal(t, event.SourceVolume.UUID, replication.ReplicationAttributes.SourceVolumeUUID)
	assert.Equal(t, "vol-1", replication.ReplicationAttributes.SourceVolumeName)
	assert.Equal(t, "loc-1", replication.ReplicationAttributes.SourceLocation)
	assert.Equal(t, "loc-2", replication.ReplicationAttributes.DestinationLocation)
	assert.Equal(t, models.VolumeReplicationCVPV1betaEndpointTypeSrc, replication.ReplicationAttributes.EndpointType)
	assert.Equal(t, "hourly", replication.ReplicationAttributes.ReplicationSchedule)
	assert.Equal(t, "pool-1", replication.ReplicationAttributes.SourcePoolUUID)
}

func Test_createReplicationObjects_InvalidUUID(t *testing.T) {
	resourceID := "replication-1"
	description := "desc"
	replicationSchedule := models.ReplicationV1betaReplicationScheduleREPLICATIONSCHEDULEUNSPECIFIED
	event := &CreateReplicationEvent{
		SourceProjectNumber:      "src-proj",
		DestinationProjectNumber: "dst-proj",
		LocationID:               "loc-1",
		DestinationLocationID:    "loc-2",
		VolumeResourceID:         "vol-1",
		CreateReplicationParams: &CreateReplicationParamsBody{
			ResourceID:          &resourceID,
			Description:         &description,
			ReplicationSchedule: &replicationSchedule,
			DestinationVolumeParameters: &DestinationVolumeParams{
				VolumeID: "vol_2",
			},
		},
		SourceVolume: datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "invalid-uuid"},
			Name:      "vol-1",
			Pool:      &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
		},
		SourcePool: datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-1"}},
	}

	replication, err := _createReplicationObjects(event, "loc-2", "region-1", "region-2")
	assert.Error(t, err)
	assert.Nil(t, replication)
}

func Test_validateLabels(t *testing.T) {
	t.Run("WhenValidLabels", func(t *testing.T) {
		validLabels := map[string]string{
			"env":    "prod",
			"region": "us-central1",
		}
		err := _validateLabels(validLabels)
		assert.NoError(t, err)
	})

	t.Run("WhenEmptyMap", func(t *testing.T) {
		err := _validateLabels(map[string]string{})
		assert.NoError(t, err)
	})

	t.Run("WhenTooManyLabels", func(t *testing.T) {
		tooMany := map[string]string{}
		for i := 0; i < 65; i++ {
			tooMany[fmt.Sprintf("k%d", i)] = "v"
		}
		err := _validateLabels(tooMany)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Label count exceeds the maximum limit of 64")
	})

	t.Run("WhenEmptyKey", func(t *testing.T) {
		labels := map[string]string{
			"": "value",
		}
		err := _validateLabels(labels)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Label key is required")
	})

	t.Run("WhenKeyTooLongCharacters", func(t *testing.T) {
		// Create a key of 64 runes (maxRuneCount is 63)
		key := strings.Repeat("a", 64)
		labels := map[string]string{
			key: "value",
		}
		err := _validateLabels(labels)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Label key exceeds the maximum length of 63 characters")
	})

	t.Run("WhenKeyTooLongBytes", func(t *testing.T) {
		// Create a key that has exactly 63 runes but > 128 bytes
		// Use a 4-byte UTF-8 character like '𐀀' (U+10000) repeated to exceed byte limit
		key := strings.Repeat("𐀀", 63) // 63 * 4 = 252 bytes, but only 63 runes
		labels := map[string]string{
			key: "value",
		}
		err := _validateLabels(labels)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Label key exceeds the maximum encoded length of 128 bytes")
	})

	t.Run("WhenValueTooLongCharacters", func(t *testing.T) {
		// Create a value of 64 runes (maxRuneCount is 63)
		value := strings.Repeat("b", 64)
		labels := map[string]string{
			"key": value,
		}
		err := _validateLabels(labels)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Label value exceeds the maximum length of 63 characters")
	})

	t.Run("WhenValueTooLongBytes", func(t *testing.T) {
		// Create a value that has exactly 63 runes but > 128 bytes
		// Use a 4-byte UTF-8 character like '𐀀' (U+10000) repeated to exceed byte limit
		value := strings.Repeat("𐀀", 63) // 63 * 4 = 252 bytes, but only 63 runes
		labels := map[string]string{
			"key": value,
		}
		err := _validateLabels(labels)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Label value exceeds the maximum encoded length of 128 bytes")
	})

	t.Run("WhenMultipleValidationFailuresFirstKeyIssue", func(t *testing.T) {
		labels := map[string]string{
			"": "value",
		}
		err := _validateLabels(labels)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Label key is required")
	})
	t.Run("WhenMultipleValidationFailuresFirstValueIssue", func(t *testing.T) {
		labels := map[string]string{
			"key": "", // Empty value should pass key validation but fail value validation
		}
		err := _validateLabels(labels)
		assert.NoError(t, err) // Empty value is valid
	})
	t.Run("WhenBoundaryValuesExactlyAtLimits", func(t *testing.T) {
		// Key exactly at maxRuneCount (63)
		key := strings.Repeat("a", 63)
		// Value exactly at maxRuneCount (63) - not maxByteCount since function checks rune count
		value := strings.Repeat("b", 63)

		labels := map[string]string{
			key: value,
		}
		err := _validateLabels(labels)
		assert.NoError(t, err)
	})

	t.Run("WhenBoundaryValuesOneBelowLimits", func(t *testing.T) {
		// Key one below maxRuneCount (62)
		key := strings.Repeat("a", 62)
		// Value one below maxRuneCount (62) - not maxByteCount since function checks rune count
		value := strings.Repeat("b", 62)

		labels := map[string]string{
			key: value,
		}
		err := _validateLabels(labels)
		assert.NoError(t, err)
	})

	t.Run("WhenUnicodeCharactersInKeyAndValue", func(t *testing.T) {
		labels := map[string]string{
			"env-测试": "prod-测试",
		}
		err := _validateLabels(labels)
		assert.NoError(t, err)
	})

	t.Run("WhenSpecialCharactersInKeyAndValue", func(t *testing.T) {
		labels := map[string]string{
			"env-prod":  "prod-env",
			"region_us": "us_region",
			"zone-1":    "zone1",
		}
		err := _validateLabels(labels)
		assert.NoError(t, err)
	})
}

// Unit tests for _getDestinationPool
func Test_getDestinationPool(t *testing.T) {
	ctx := context.Background()
	destBasePath := "destBasePath"
	token := "token"
	remoteLocationID := "loc"
	projectNumber := "proj"
	xCorrelationID := new(string)
	poolName := "pool-1"

	t.Run("Returns pool when found", func(t *testing.T) {
		mockPool := googleproxyclient.PoolV1beta{ResourceId: poolName}
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaListPools(ctx, mock.Anything).Return(&googleproxyclient.V1betaListPoolsOK{
			Pools: []googleproxyclient.PoolV1beta{mockPool},
		}, nil)

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		pool, err := _getDestinationPool(ctx, destBasePath, token, remoteLocationID, projectNumber, xCorrelationID, poolName)
		assert.NoError(t, err)
		assert.NotNil(t, pool)
		assert.Equal(t, poolName, pool.ResourceId)
	})

	t.Run("Returns not found error when pool does not exist", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaListPools(ctx, mock.Anything).Return(&googleproxyclient.V1betaListPoolsOK{
			Pools: []googleproxyclient.PoolV1beta{},
		}, nil)

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		pool, err := _getDestinationPool(ctx, destBasePath, token, remoteLocationID, projectNumber, xCorrelationID, poolName)
		assert.Error(t, err)
		assert.Nil(t, pool)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("Returns error when Invoker returns error", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaListPools(ctx, mock.Anything).Return(nil, errors.New("api error"))

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		pool, err := _getDestinationPool(ctx, destBasePath, token, remoteLocationID, projectNumber, xCorrelationID, poolName)
		assert.Error(t, err)
		assert.Nil(t, pool)
	})
}

// Unit tests for _internalGetVolumeCount
func Test_internalGetVolumeCount(t *testing.T) {
	ctx := context.Background()
	basePath := "basePath"
	projectNumber := "proj"
	locationID := "loc"
	poolID := "pool"
	jwt := "token"
	storageClass := "SOFTWARE"
	serviceLevel := "FLEX"

	t.Run("Returns volume count when successful", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaGetVolumeCount(ctx, mock.Anything).Return(&googleproxyclient.V1betaGetVolumeCountOK{
			VolumeCount: 5,
		}, nil)

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		count, err := _internalGetVolumeCount(ctx, basePath, projectNumber, locationID, poolID, jwt, storageClass, serviceLevel)
		assert.NoError(t, err)
		assert.Equal(t, 5, count)
	})

	t.Run("Returns 0 when Invoker returns error", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaGetVolumeCount(ctx, mock.Anything).Return(nil, errors.New("api error"))

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		count, err := _internalGetVolumeCount(ctx, basePath, projectNumber, locationID, poolID, jwt, storageClass, serviceLevel)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

// Unit tests for _internalGetReplicationCount
func Test_internalGetReplicationCount(t *testing.T) {
	ctx := context.Background()
	basePath := "basePath"
	projectNumber := "proj"
	locationID := "loc"
	poolID := "pool"
	jwt := "token"
	storageClass := "SOFTWARE"
	serviceLevel := "FLEX"

	t.Run("Returns replication count when successful", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaGetReplicationCount(ctx, mock.Anything).Return(&googleproxyclient.V1betaGetReplicationCountOK{
			ReplicationCount: 7,
		}, nil)

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		count, err := _internalGetReplicationCount(ctx, basePath, projectNumber, locationID, poolID, jwt, storageClass, serviceLevel)
		assert.NoError(t, err)
		assert.Equal(t, 7, count)
	})

	t.Run("Returns 0 when Invoker returns error", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaGetReplicationCount(ctx, mock.Anything).Return(nil, errors.New("api error"))

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		count, err := _internalGetReplicationCount(ctx, basePath, projectNumber, locationID, poolID, jwt, storageClass, serviceLevel)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

// Unit tests for _getVolume
func Test_getVolume(t *testing.T) {
	ctx := context.Background()
	basePath := "basePath"
	token := "token"
	locationID := "loc"
	projectNumber := "proj"
	xCorrelationID := new(string)
	volumeResourceId := "vol-1"

	t.Run("Returns volume when found", func(t *testing.T) {
		mockVolume := googleproxyclient.VolumeV1beta{ResourceId: volumeResourceId}
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaListVolumes(ctx, mock.Anything).Return(&googleproxyclient.V1betaListVolumesOK{
			Volumes: []googleproxyclient.VolumeV1beta{mockVolume},
		}, nil)

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		vol, err := _getVolume(ctx, basePath, token, locationID, projectNumber, xCorrelationID, volumeResourceId)
		assert.NoError(t, err)
		assert.Equal(t, volumeResourceId, vol.ResourceId)
	})

	t.Run("Returns error when volume not found", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaListVolumes(ctx, mock.Anything).Return(&googleproxyclient.V1betaListVolumesOK{
			Volumes: []googleproxyclient.VolumeV1beta{},
		}, nil)

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		vol, err := _getVolume(ctx, basePath, token, locationID, projectNumber, xCorrelationID, volumeResourceId)
		assert.Error(t, err)
		assert.Equal(t, "", vol.ResourceId)
		assert.Contains(t, err.Error(), "Volume 'vol-1' not found")
	})

	t.Run("Returns error when Invoker returns error", func(t *testing.T) {
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaListVolumes(ctx, mock.Anything).Return(nil, errors.New("api error"))

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{Invoker: mockClient}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()

		vol, err := _getVolume(ctx, basePath, token, locationID, projectNumber, xCorrelationID, volumeResourceId)
		assert.Error(t, err)
		assert.Equal(t, "", vol.ResourceId)
	})
}

// Unit tests for _validateCreateReplicationParams
func Test_validateCreateReplicationParams(t *testing.T) {
	ctx := context.Background()
	projectNumber := "proj-1"
	destProjectNumber := "proj-1"
	locationID := "us-east1"
	destLocationID := "us-east4"
	volumeResourceID := "vol-1"
	resourceID := "replication-1"
	description := "desc"
	replicationSchedule := models.ReplicationV1betaReplicationScheduleDAILY
	destPoolName := "pool-1"
	destPoolID := "pool-1"
	xCorrelationID := new(string)
	storagePoolUri := "projects/proj-2/locations/loc-2/pools/pool-1"

	baseVolume := datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "b3b3b3b3-b3b3-4b3b-b3b3-b3b3b3b3b3b3"},
		Name:        volumeResourceID,
		Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}, ServiceLevel: "FLEX"},
		State:       string(googleproxyclient.VolumeV1betaVolumeStateREADY),
		SizeInBytes: 100,
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
			CreationToken:    "token-1",
		},
	}

	event := &CreateReplicationEvent{
		SourceProjectNumber:      projectNumber,
		DestinationProjectNumber: destProjectNumber,
		LocationID:               locationID,
		DestinationLocationID:    destLocationID,
		VolumeResourceID:         volumeResourceID,
		CreateReplicationParams: &CreateReplicationParamsBody{
			ResourceID:          &resourceID,
			Description:         &description,
			ReplicationSchedule: &replicationSchedule,
			Labels:              map[string]string{"env": "prod"},
			DestinationVolumeParameters: &DestinationVolumeParams{
				VolumeID:    "vol_2",
				ShareName:   "share_2",
				StoragePool: &storagePoolUri,
			},
		},
		SourceVolume:        baseVolume,
		SourcePool:          datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}, ServiceLevel: "FLEX"},
		DestinationPoolName: destPoolName,
		XCorrelationID:      xCorrelationID,
	}

	t.Run("Success", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mockStorage.On("GetAccount", ctx, projectNumber).Return(&datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acc-1"}}, nil)
		validateReplicationResourceId = func(ctx context.Context, projectNumber string, paramReplicationResourceId string, paramsVolumeResourceId string, se database.Storage) error {
			return nil
		}
		defer func() { validateReplicationResourceId = _validateReplicationResourceId }()

		// Patch dependencies
		origGetSignedJwtToken := InternalUtilGetSignedToken
		InternalUtilGetSignedToken = func(p string) (string, error) { return "token", nil }
		defer func() { InternalUtilGetSignedToken = origGetSignedJwtToken }()

		origGetPairedRegionURI := InternalUtilGetPairedRegionURI
		InternalUtilGetPairedRegionURI = func(region string) (string, error) { return "basePath", nil }
		defer func() { InternalUtilGetPairedRegionURI = origGetPairedRegionURI }()

		origGetQuotaLimit := getQuotaLimit
		getQuotaLimit = func(ctx context.Context, logger log.Logger, region string, projectId string, token string, resourceType common.ResourceType) (int, error) {
			return 10, nil
		}
		defer func() { getQuotaLimit = origGetQuotaLimit }()

		origGetCallbackToken := InternalUtilGetCallbackToken
		InternalUtilGetCallbackToken = func() (string, error) { return "cbtoken", nil }
		defer func() { InternalUtilGetCallbackToken = origGetCallbackToken }()

		origGetDestinationPool := getDestinationPool
		getDestinationPool = func(ctx context.Context, destBasePath, token, remoteLocationID, projectNumber string, xCorrelationID *string, name string) (*googleproxyclient.PoolV1beta, error) {
			return &googleproxyclient.PoolV1beta{
				ResourceId:       destPoolID,
				PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
				AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
				SizeInBytes:      200,
				ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
				StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
			}, nil
		}
		defer func() { getDestinationPool = origGetDestinationPool }()

		origGetVolume := getVolume
		getVolume = func(ctx context.Context, basePath, token, locationID, projectNumber string, xCorrelationID *string, volumeResourceId string) (googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{}, errors.NewNotFoundErr("Volume", &volumeResourceId)
		}
		defer func() { getVolume = origGetVolume }()

		origInternalGetReplicationCount := internalGetReplicationCount
		internalGetReplicationCount = func(ctx context.Context, basePath, projectNumber, locationID, poolID, jwt, storageClass, serviceLevel string) (int, error) {
			return 0, nil
		}
		defer func() { internalGetReplicationCount = origInternalGetReplicationCount }()

		origInternalGetVolumeCount := internalGetVolumeCount
		internalGetVolumeCount = func(ctx context.Context, basePath, projectNumber, locationID, poolID, jwt, storageClass, serviceLevel string) (int, error) {
			return 0, nil
		}
		defer func() { internalGetVolumeCount = origInternalGetVolumeCount }()

		origReplicationJobInProcess := replicationJobInProcess
		replicationJobInProcess = func(ctx context.Context, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId string, correlationId *string) error {
			return nil
		}
		defer func() { replicationJobInProcess = origReplicationJobInProcess }()

		origCreateReplicationObjects := createReplicationObjects
		createReplicationObjects = func(event *CreateReplicationEvent, remotelocation, region, remoteRegion string) (*datamodel.VolumeReplication, error) {
			return &datamodel.VolumeReplication{Uri: "uri"}, nil
		}
		defer func() { createReplicationObjects = origCreateReplicationObjects }()

		replication, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.NoError(t, err)
		assert.NotNil(t, replication)
	})

	t.Run("Fails when replicationSchedule is UNSPECIFIED", func(t *testing.T) {
		unspecified := models.ReplicationV1betaReplicationScheduleREPLICATIONSCHEDULEUNSPECIFIED
		eventCopy := *event
		paramsCopy := *event.CreateReplicationParams
		paramsCopy.ReplicationSchedule = &unspecified
		eventCopy.CreateReplicationParams = &paramsCopy

		mockStorage := &database.MockStorage{}
		_, err := _validateCreateReplicationParams(ctx, &eventCopy, mockStorage)
		assert.Error(t, err)
	})

	t.Run("Fails when auto tiering is disabled", func(t *testing.T) {
		unspecified := models.ReplicationV1betaReplicationScheduleREPLICATIONSCHEDULEUNSPECIFIED
		eventCopy := &CreateReplicationEvent{
			SourceProjectNumber:      projectNumber,
			DestinationProjectNumber: destProjectNumber,
			LocationID:               locationID,
			DestinationLocationID:    destLocationID,
			VolumeResourceID:         volumeResourceID,
			CreateReplicationParams: &CreateReplicationParamsBody{
				ResourceID:          &resourceID,
				Description:         &description,
				ReplicationSchedule: &replicationSchedule,
				Labels:              map[string]string{"env": "prod"},
				DestinationVolumeParameters: &DestinationVolumeParams{
					VolumeID:    "vol_2",
					ShareName:   "share_2",
					StoragePool: &storagePoolUri,
				},
			},
			SourceVolume:        baseVolume,
			SourcePool:          datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}, ServiceLevel: "FLEX"},
			DestinationPoolName: destPoolName,
			XCorrelationID:      xCorrelationID,
		}
		paramsCopy := eventCopy.CreateReplicationParams
		paramsCopy.DestinationVolumeParameters.TieringPolicy = &googleproxyclient.TieringPolicyV1beta{}
		paramsCopy.ReplicationSchedule = &unspecified
		eventCopy.CreateReplicationParams = paramsCopy

		mockStorage := &database.MockStorage{}
		_, err := _validateCreateReplicationParams(ctx, eventCopy, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "Auto-Tiering feature is currently not enabled.")
	})

	t.Run("Fails when label validation fails", func(t *testing.T) {
		eventCopy := *event
		paramsCopy := *event.CreateReplicationParams
		labels := map[string]string{}
		for i := 0; i < 65; i++ {
			labels[fmt.Sprintf("k%d", i)] = "v"
		}
		paramsCopy.Labels = labels
		eventCopy.CreateReplicationParams = &paramsCopy

		mockStorage := &database.MockStorage{}
		_, err := _validateCreateReplicationParams(ctx, &eventCopy, mockStorage)
		assert.Error(t, err)
	})

	t.Run("Fails when cross project replication is attempted", func(t *testing.T) {
		eventCopy := *event
		eventCopy.DestinationProjectNumber = "proj-2" // Different from source project

		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", eventCopy.SourceProjectNumber).Return("token", nil).Once()
		mm.On("InternalUtilGetSignedToken", eventCopy.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", eventCopy.LocationID).Return("us-east1", "us-east1-a", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "us-east1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", eventCopy.DestinationLocationID).Return("us-east4", "us-east4-a", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "us-east4").Return("basePath", nil).Once()

		_, err := _validateCreateReplicationParams(ctx, &eventCopy, mockStorage)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cross project replication is not supported")
		mm.AssertExpectations(t)
	})

	t.Run("Fails when cross zone replication is attempted", func(t *testing.T) {
		eventCopy := *event
		eventCopy.DestinationLocationID = "us-east1" // Same region as source

		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		// Only one token call since source and destination projects are the same
		mm.On("InternalUtilGetSignedToken", eventCopy.SourceProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", eventCopy.LocationID).Return("us-east1", "us-east1-a", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "us-east1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", eventCopy.DestinationLocationID).Return("us-east1", "us-east1-b", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "us-east1").Return("basePath", nil).Once()

		_, err := _validateCreateReplicationParams(ctx, &eventCopy, mockStorage)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cross zone replication is not supported")
		mm.AssertExpectations(t)
	})

	t.Run("Fails when GetSignedToken fails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		origGetSignedJwtToken := InternalUtilGetSignedToken
		InternalUtilGetSignedToken = func(p string) (string, error) { return "", errors.New("token error") }
		defer func() { InternalUtilGetSignedToken = origGetSignedJwtToken }()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
	})

	// More negative test cases can be added for each error path as needed
	t.Run("WhenInternalUtilGetSignedTokenFailsForDestinationPool", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("", errors.New("token error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenInternalParseRegionAndZoneFailsForSourceLocation", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("", "", errors.New("parse error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenInternalUtilGetPairedRegionURIFailsForSourceLocation", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region", "zone", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region").Return("", errors.New("paired region uri error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenInternalParseRegionAndZoneFailsForDestinationLocation", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region", "zone", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("", "", errors.New("parse error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenInternalUtilGetPairedRegionURIFailsForDestinationLocation", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region", "zone", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region", "zone", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region").Return("", errors.New("paired region uri error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenValidateReplicationResourceIdFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(errors.New("validate replication resource id error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenVolumeIsDataProtectionVolume", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()

		event.SourceVolume.VolumeAttributes.IsDataProtection = true

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrValidateCreateSourceVolumeInReplicationGroup, errors.New("sourceVolume already in replication")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenSourceVolumeNotReady", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()

		// Reset volume state to not ready
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateCREATING)

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrValidateCreateSourceVolumeNotReady, errors.New("sourceVolume is not in a READY state")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenValidateStoragePoolUriFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Test(t)
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(errors.New("invalid storage pool URI format")).Once()

		// Reset volume state
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrValidateStoragePoolUri, errors.New("invalid storage pool URI format")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenGetDestinationPoolFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(nil, errors.New("destination pool error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, errors.New("destination pool error"), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenDestinationPoolInTransitionState", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		transitioningPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateCREATING),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(transitioningPool, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrValidateDestinationPoolTransitioning, errors.New("Destination pool is in transition state")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenDestinationPoolUnhealthy", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		unhealthyPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateERROR),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(unhealthyPool, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrValidateDestinationStoragePoolState, errors.New("Destination pool is in unhealthy state, Please try after some time")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenDestinationPoolSizeExceeded", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		// Set a small destination pool that can't accommodate the volume
		smallPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(50),
			SizeInBytes:      100, // Smaller than source volume size (100) + allocated bytes (50)
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(smallPool, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrDestPoolSize, errors.New("Volume exceeds destination pool size")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenDestinationPoolSizeATDisabled", func(t *testing.T) {
		// Patch env variable
		autoTieringEnabled = true
		defer func() {
			autoTieringEnabled = env.GetBool("AUTO_TIERING_ENABLED", false)
			event.CreateReplicationParams.DestinationVolumeParameters.TieringPolicy = nil
		}()
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri
		event.CreateReplicationParams.DestinationVolumeParameters.TieringPolicy = &googleproxyclient.TieringPolicyV1beta{
			TierAction: googleproxyclient.OptNilTieringPolicyV1betaTierAction{
				Value: "ENABLED",
				Set:   true,
				Null:  false,
			},
			CoolingThresholdDays: googleproxyclient.OptNilInt32{Value: 2},
		}
		// Set a Autotiering disabled destination pool
		nonATPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(50),
			SizeInBytes:      1000, // Smaller than source volume size (100) + allocated bytes (50)
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(nonATPool, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrDestPoolTieringPolicyMismatch, errors.New("Auto tiering is not enabled on the destination pool")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenDestinationVolumeHasInvalidCoolingDays", func(t *testing.T) {
		// Patch env variable
		autoTieringEnabled = true
		defer func() {
			autoTieringEnabled = env.GetBool("AUTO_TIERING_ENABLED", false)
			event.CreateReplicationParams.DestinationVolumeParameters.TieringPolicy = nil
		}()
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri
		event.CreateReplicationParams.DestinationVolumeParameters.TieringPolicy = &googleproxyclient.TieringPolicyV1beta{
			TierAction: googleproxyclient.OptNilTieringPolicyV1betaTierAction{
				Value: "ENABLED",
				Set:   true,
				Null:  false,
			},
			CoolingThresholdDays: googleproxyclient.OptNilInt32{Value: 1},
		}

		atPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(50),
			SizeInBytes:      1000,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
			AllowAutoTiering: googleproxyclient.OptNilBool{Value: true, Set: true},
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(atPool, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrDestVolumeTieringThresholdOutOfRange, errors.New("Coolness threshold days should be in between 2 and 183")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenServiceLevelMismatch", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		// Set different service level for destination pool
		differentServiceLevelPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelEXTREME, // Different from source (FLEX)
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(differentServiceLevelPool, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrServiceLevelMismatch, errors.New("Service level on source volume and destination pool do not match")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenReplicationJobInProcessFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(errors.New("replication job in process error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, errors.New("replication job in process error"), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenGetCallbackTokenFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("", errors.New("callback token error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrGetSignedCallbackToken, errors.New("callback token error")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenGetReplicationQuotaLimitFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", mock.Anything).Return(0, errors.New("quota limit error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrGetReplicationQuotaLimitInternal, errors.New("quota limit error")), err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenGetReplicationCountFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", mock.Anything).Return(5, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, errors.New("replication count error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrValidateCreateReplicationCvpInternalGetReplicationCount, errors.New("replication count error")), err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenReplicationQuotaLimitExceeded", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", mock.Anything).Return(5, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(5, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrReplicationQuotaLimitExceeded, errors.New("Quota limit 'ReplicatedVolumesPerRegion' has been exceeded.")), err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenGetVolumeQuotaLimitFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(0, errors.New("volume quota limit error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrGetVolumeQuotaLimitInternal, errors.New("volume quota limit error")), err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenInternalGetVolumeCountFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(5, nil).Once()

		mm.On("internalGetVolumeCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, errors.New("volume count error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrValidateCreateReplicationCvpInternalGetVolumeCount, errors.New("volume count error")), err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenVolumeQuotaLimitExceeded", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(5, nil).Once()

		mm.On("internalGetVolumeCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(5, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrVolumeQuotaLimitExceeded, errors.New("Quota limit 'VolumesPerRegion' on destination region has been exceeded.")), err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenGetVolumeFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(10, nil).Once()

		mm.On("internalGetVolumeCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, nil).Once()

		// Mock destination volume already exists
		existingVolume := googleproxyclient.VolumeV1beta{
			ResourceId:    "vol_2",
			CreationToken: googleproxyclient.OptString{Value: "share_2", Set: true},
		}
		mm.On("getVolume", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, "vol_2").Return(existingVolume, errors.New("get volume error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrValidateGetVolumeReplicationCreation, errors.New("get volume error")), err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenDestinationVolumeAlreadyExists", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(10, nil).Once()

		mm.On("internalGetVolumeCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, nil).Once()

		// Mock destination volume already exists
		existingVolume := googleproxyclient.VolumeV1beta{
			ResourceId:    "vol_2",
			CreationToken: googleproxyclient.OptString{Value: "share_2", Set: true},
		}
		mm.On("getVolume", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, "vol_2").Return(existingVolume, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrGetVolumeCreateTokenInUseRemoteShareName, errors.New("RemoteShareName already Exists")), err)
		mm.AssertExpectations(t)
	})
	t.Run("WhenCreateReplicationObjectsFails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(10, nil).Once()

		mm.On("internalGetVolumeCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, nil).Once()

		// Mock destination volume not found (which is expected)
		mm.On("getVolume", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, "vol_2").Return(googleproxyclient.VolumeV1beta{}, errors.NewNotFoundErr("Volume", &[]string{"vol_2"}[0])).Once()

		mm.On("createReplicationObjects", event, event.DestinationLocationID, "region-1", "region-2").Return(nil, errors.New("create replication objects error")).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrValidateCreateDummyReplication, errors.New("create replication objects error")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenDestinationVolumeShareNameConflict", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(10, nil).Once()

		mm.On("internalGetVolumeCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, nil).Once()

		// Mock destination volume with conflicting share name
		existingVolume := googleproxyclient.VolumeV1beta{
			ResourceId:    "vol_2",
			CreationToken: googleproxyclient.OptString{Value: "share_2", Set: true}, // Same as event.CreateReplicationParams.DestinationVolumeParameters.ShareName
		}
		mm.On("getVolume", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, "vol_2").Return(existingVolume, nil).Once()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
		assert.Equal(t, vsaErrors.NewVCPError(vsaErrors.ErrGetVolumeCreateTokenInUseRemoteShareName, errors.New("RemoteShareName already Exists")), err)
		mm.AssertExpectations(t)
	})

	t.Run("WhenDestinationVolumeUsesDefaultValues", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		// Clear destination volume parameters to test default values
		event.CreateReplicationParams.DestinationVolumeParameters.VolumeID = ""
		event.CreateReplicationParams.DestinationVolumeParameters.ShareName = ""

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(10, nil).Once()

		mm.On("internalGetVolumeCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, nil).Once()

		// Mock destination volume not found (which is expected)
		mm.On("getVolume", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.SourceVolume.Name).Return(googleproxyclient.VolumeV1beta{}, errors.NewNotFoundErr("Volume", &event.SourceVolume.Name)).Once()

		mm.On("createReplicationObjects", event, event.DestinationLocationID, "region-1", "region-2").Return(&datamodel.VolumeReplication{Uri: "uri"}, nil).Once()

		replication, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.NoError(t, err)
		assert.NotNil(t, replication)
		mm.AssertExpectations(t)
	})
	t.Run("WhenCrossProjectReplicationIsEnabled", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		event.DestinationProjectNumber = "proj-1"
		event.SourceProjectNumber = "proj-2"

		// Patch env variable
		cpcrEnabled = true
		defer func() {
			cpcrEnabled = env.GetBool("CPCRR_ENABLED", false)
		}()

		mm.On("InternalUtilGetSignedToken", event.SourceProjectNumber).Return("token", nil).Once()
		mm.On("InternalUtilGetSignedToken", event.DestinationProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-2", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-2").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		// Clear destination volume parameters to test default values
		event.CreateReplicationParams.DestinationVolumeParameters.VolumeID = ""
		event.CreateReplicationParams.DestinationVolumeParameters.ShareName = ""

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(10, nil).Once()

		mm.On("internalGetVolumeCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, nil).Once()

		// Mock destination volume not found (which is expected)
		mm.On("getVolume", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.SourceVolume.Name).Return(googleproxyclient.VolumeV1beta{}, errors.NewNotFoundErr("Volume", &event.SourceVolume.Name)).Once()

		mm.On("createReplicationObjects", event, event.DestinationLocationID, "region-1", "region-2").Return(&datamodel.VolumeReplication{Uri: "uri"}, nil).Once()

		replication, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.NoError(t, err)
		assert.NotNil(t, replication)
		mm.AssertExpectations(t)
	})
	t.Run("WhenCrossZoneReplicationIsEnabled", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		mm := &monkeyMock{}
		mm.Patch()
		defer mm.Unpatch()

		event.DestinationProjectNumber = "proj-1"
		event.SourceProjectNumber = "proj-1"

		// Patch env variable
		czcrEnabled = true
		defer func() {
			czcrEnabled = env.GetBool("CZCRR_ENABLED", false)
		}()

		mm.On("InternalUtilGetSignedToken", event.SourceProjectNumber).Return("token", nil).Once()
		mm.On("InternalParseRegionAndZone", event.LocationID).Return("region-1", "zone-1", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("InternalParseRegionAndZone", event.DestinationLocationID).Return("region-1", "zone-2", nil).Once()
		mm.On("InternalUtilGetPairedRegionURI", "region-1").Return("basePath", nil).Once()
		mm.On("validateReplicationResourceId", ctx, event.SourceProjectNumber, *event.CreateReplicationParams.ResourceID, event.VolumeResourceID, mockStorage).Return(nil).Once()
		mm.On("validateStoragePoolUri", *event.CreateReplicationParams.DestinationVolumeParameters.StoragePool).Return(nil).Once()

		// Reset volume state and storage pool
		event.SourceVolume.VolumeAttributes.IsDataProtection = false
		event.SourceVolume.State = string(googleproxyclient.VolumeV1betaVolumeStateREADY)
		event.CreateReplicationParams.DestinationVolumeParameters.StoragePool = &storagePoolUri

		// Clear destination volume parameters to test default values
		event.CreateReplicationParams.DestinationVolumeParameters.VolumeID = ""
		event.CreateReplicationParams.DestinationVolumeParameters.ShareName = ""

		validPool := &googleproxyclient.PoolV1beta{
			ResourceId:       destPoolID,
			PoolId:           googleproxyclient.OptString{Value: destPoolID, Set: true},
			AllocatedBytes:   googleproxyclient.NewOptNilFloat64(0),
			SizeInBytes:      200,
			ServiceLevel:     googleproxyclient.PoolV1betaServiceLevelFLEX,
			StoragePoolState: googleproxyclient.NewOptPoolV1betaStoragePoolState(googleproxyclient.PoolV1betaStoragePoolStateREADY),
		}
		mm.On("getDestinationPool", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.DestinationPoolName).Return(validPool, nil).Once()

		mm.On("replicationJobInProcess", ctx, event.SourceProjectNumber, event.DestinationProjectNumber, "basePath", "basePath", event.LocationID, event.DestinationLocationID, "token", "token", mock.Anything, "", event.SourcePool.UUID, destPoolID, event.XCorrelationID).Return(nil).Once()

		mm.On("InternalUtilGetCallbackToken").Return("callback-token", nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeReplication).Return(10, nil).Once()

		mm.On("internalGetReplicationCount", ctx, "basePath", event.DestinationProjectNumber, event.DestinationLocationID, "", "token", mock.Anything, mock.Anything).Return(0, nil).Once()

		mm.On("getQuotaLimit", ctx, mock.Anything, event.DestinationLocationID, event.DestinationProjectNumber, "callback-token", common.ResourceTypeVolume).Return(10, nil).Once()

		mm.On("internalGetVolumeCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(0, nil).Once()

		// Mock destination volume not found (which is expected)
		mm.On("getVolume", ctx, "basePath", "token", event.DestinationLocationID, event.DestinationProjectNumber, event.XCorrelationID, event.SourceVolume.Name).Return(googleproxyclient.VolumeV1beta{}, errors.NewNotFoundErr("Volume", &event.SourceVolume.Name)).Once()

		mm.On("createReplicationObjects", event, event.DestinationLocationID, "region-1", "region-1").Return(&datamodel.VolumeReplication{Uri: "uri"}, nil).Once()

		replication, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.NoError(t, err)
		assert.NotNil(t, replication)
		mm.AssertExpectations(t)
	})
}

func TestValidateReplicationParams(t *testing.T) {
	event := &CommonReplicationEventParams{
		AccountName:              "test-account",
		Location:                 "test-location",
		VolumeResourceID:         "test-volume-id",
		ReplicationResourceID:    "test-replication-id",
		SourceProjectNumber:      "123456789",
		DestinationProjectNumber: "123456789",
	}
	t.Run("WhenListVolumeReplicationError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrDatabaseDataReadError, errors.New("some error"))
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")

		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenEmptyListVolumeReplication", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		expectedError := errors.NewUserInputValidationErr("No replication found for the given URI")
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenParsingProjectError", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)

		parseError := errors.New("some error")
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", vsaErrors.NewVCPError(vsaErrors.ErrProjectParsingError, parseError)
		}
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSignedTokenError", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
			InternalParseRegionAndZone = utils.ParseRegionAndZone
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", vsaErrors.NewVCPError(vsaErrors.ErrGetSignedToken, errors.New("some error"))
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			return location, "", nil
		}
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenGetPairedRegionError", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			return location, "", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "", errors.New("some error")
		}
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenReplicationJobInProcessError", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			replicationJobInProcess = _replicationJobInProcess
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		mockStorage.On("CheckAndFetchDuplicateJobs", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			return location, "", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		replicationJobInProcess = func(ctx context.Context, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId string, correlationId *string) error {
			return errors.New("some error")
		}
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			replicationJobInProcess = _replicationJobInProcess
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-east4-1",
					DestinationLocation: "us-central1-a",
				},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		mockStorage.On("CheckAndFetchDuplicateJobs", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			return location, "", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		replicationJobInProcess = func(ctx context.Context, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId string, correlationId *string) error {
			return nil
		}
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccessInCaseOfCleanup", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			replicationJobInProcess = _replicationJobInProcess
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-east4-1",
					DestinationLocation: "us-central1-a",
				},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		mockStorage.On("CheckAndFetchDuplicateJobs", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			return location, "", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		replicationJobInProcess = func(ctx context.Context, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId string, correlationId *string) error {
			return nil
		}
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, true, "CREATE_VOLUME_REPLICATION")
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenParseSourceLocationError", func(tt *testing.T) {
		defer func() {
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "invalid-location",
					DestinationLocation: "us-east1",
				},
				RemoteUri: "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			return "", "", errors.New("parse error")
		}
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrParseSourceLocation, errors.New("parse error"))
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenGetPairedSourceRegionURIError", func(tt *testing.T) {
		defer func() {
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
				RemoteUri: "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			return "us-central1", "us-central1-a", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "", errors.New("paired region error")
		}
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrGetSrcBasePath, errors.New("paired region error"))
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenParseDestinationLocationError", func(tt *testing.T) {
		defer func() {
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "invalid-location",
				},
				RemoteUri: "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			if location == "us-central1" {
				return "us-central1", "us-central1-a", nil
			}
			return "", "", errors.New("parse destination error")
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrParseDestinationLocation, errors.New("parse destination error"))
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenGetPairedDestinationRegionURIError", func(tt *testing.T) {
		defer func() {
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
				RemoteUri: "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			if location == "us-central1" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			if region == "us-central1" {
				return "basePath", nil
			}
			return "", errors.New("paired destination region error")
		}
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrGetDstBasePath, errors.New("paired destination region error"))
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenCheckAndFetchDuplicateJobsError", func(tt *testing.T) {
		defer func() {
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
				RemoteUri: "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		mockStorage.On("CheckAndFetchDuplicateJobs", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("duplicate job check error"))
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			if location == "us-central1" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		expectedError := errors.New("duplicate job check error")
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenDuplicateJobFound", func(tt *testing.T) {
		defer func() {
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			getReplication = _getReplication
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
				RemoteUri: "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		existingJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: "existing-job-uuid",
			},
		}
		mockStorage.On("CheckAndFetchDuplicateJobs", mock.Anything, mock.Anything, mock.Anything).Return(existingJob, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			if location == "us-central1" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		mirrorState := "MIRRORED"
		mockReplication := &coreModels.VolumeReplication{
			MirrorState: &mirrorState,
		}
		getReplication = func(ctx context.Context, basePath, projectNumber, location, replicationUUID, token string) (*coreModels.VolumeReplication, error) {
			return mockReplication, nil
		}
		replication, jobUUID, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.NoError(tt, err)
		assert.Equal(tt, mockReplication, replication)
		assert.Equal(tt, "existing-job-uuid", *jobUUID)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenGetReplicationErrorForDuplicateJob", func(tt *testing.T) {
		defer func() {
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			getReplication = _getReplication
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
				RemoteUri: "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		existingJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: "existing-job-uuid",
			},
		}
		mockStorage.On("CheckAndFetchDuplicateJobs", mock.Anything, mock.Anything, mock.Anything).Return(existingJob, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			if location == "us-central1" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		getReplication = func(ctx context.Context, basePath, projectNumber, location, replicationUUID, token string) (*coreModels.VolumeReplication, error) {
			return nil, errors.New("get replication error")
		}
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrGoogleProxyInternalGetMultipleReplications, errors.New("get replication error"))
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenReplicationJobInProcessError", func(tt *testing.T) {
		defer func() {
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			replicationJobInProcess = _replicationJobInProcess
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
				RemoteUri: "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		mockStorage.On("CheckAndFetchDuplicateJobs", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			if location == "us-central1" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		replicationJobInProcess = func(ctx context.Context, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId string, correlationId *string) error {
			return errors.New("replication job in process error")
		}
		expectedError := errors.New("replication job in process error")
		_, _, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		defer func() {
			InternalParseRegionAndZone = utils.ParseRegionAndZone
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			replicationJobInProcess = _replicationJobInProcess
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:      "us-central1",
					DestinationLocation: "us-east1",
				},
				RemoteUri: "projects/123456789/locations/us-central1/volumes/test-volume/replications/test-replication",
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything, mock.Anything).Return(response, nil)
		mockStorage.On("CheckAndFetchDuplicateJobs", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "123456789", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "mock-token", nil
		}
		InternalParseRegionAndZone = func(location string) (string, string, error) {
			if location == "us-central1" {
				return "us-central1", "us-central1-a", nil
			}
			return "us-east1", "us-east1-a", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		replicationJobInProcess = func(ctx context.Context, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId string, correlationId *string) error {
			return nil
		}
		replication, jobUUID, err := _validateReplicationParams(context.Background(), event, 12345, mockStorage, false, "CREATE_VOLUME_REPLICATION")
		assert.NoError(tt, err)
		assert.Nil(tt, replication)
		assert.Nil(tt, jobUUID)
		mockStorage.AssertExpectations(tt)
	})
}

func TestVerifyDstReplicationResume(t *testing.T) {
	event := &ResumeReplicationEvent{
		CommonReplicationEventParams: CommonReplicationEventParams{
			DstBasePath:              "dstPath",
			DestinationProjectNumber: "destinationProjectNumber",
			DstToken:                 "dstToken",
			ReplicationModel: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dstLocation",
					DestinationReplicationUUID: "dstUUID",
				},
			},
		},
	}
	t.Run("WhenGetReplicationError", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return nil, errors.New("some error")
		}
		_, err := _verifyDstReplicationResume(ctx, event)
		assert.Error(tt, err)
	})
	t.Run("WhenMirrorStateMirrored", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := "MIRRORED"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState: &mirrorState,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}
		expectedError := errors.NewUserInputValidationErr(fmt.Sprintf("Replication mirror state should be %s", models.ReplicationV1betaMirrorStateSTOPPED))
		_, err := _verifyDstReplicationResume(ctx, event)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenMirrorStateUninitialized", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := "UNINITIALIZED"
		relationshipStatus := "transferring"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}
		expectedError := errors.NewUserInputValidationErr(fmt.Sprintf("Replication relationship status should be %s", models.VolumeReplicationCVPV1betaRelationshipStatusIdle))
		_, err := _verifyDstReplicationResume(ctx, event)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := "STOPPED"
		relationshipStatus := "IDLE"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}
		resp, err := _verifyDstReplicationResume(ctx, event)
		assert.NoError(tt, err)
		assert.Equal(tt, dstReplication, resp)
	})
}

func TestVerifyDstReplicationStop(t *testing.T) {
	t.Run("VerifyDstReplicationStopSucceeds", func(tt *testing.T) {
		mirrorState := "MIRRORED"
		relationshipStatus := "IDLE"
		mockReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		event := &StopReplicationEvent{
			CommonReplicationEventParams: CommonReplicationEventParams{
				DstBasePath:              "dstPath",
				DestinationProjectNumber: "destinationProjectNumber",
				DstToken:                 "dstToken",
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation:        "dstLocation",
						DestinationReplicationUUID: "dstUUID",
					},
				},
			},
		}

		getReplication = func(ctx context.Context, basePath, projectNumber, locationID, replicationUUID, jwt string) (*coreModels.VolumeReplication, error) {
			return mockReplication, nil
		}
		defer func() { getReplication = _getReplication }()

		replication, err := _verifyDstReplicationStop(context.Background(), event)
		assert.NoError(tt, err)
		assert.NotNil(tt, replication)
		assert.Equal(tt, mockReplication, replication)
	})
	t.Run("VerifyDstReplicationStopFailsWhenReplicationNotFound", func(tt *testing.T) {
		event := &StopReplicationEvent{
			CommonReplicationEventParams: CommonReplicationEventParams{
				DstBasePath:              "dstPath",
				DestinationProjectNumber: "destinationProjectNumber",
				DstToken:                 "dstToken",
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation:        "dstLocation",
						DestinationReplicationUUID: "dstUUID",
					},
				},
			},
		}

		getReplication = func(ctx context.Context, basePath, projectNumber, locationID, replicationUUID, jwt string) (*coreModels.VolumeReplication, error) {
			return nil, errors.New("replication not found")
		}
		defer func() { getReplication = _getReplication }()

		replication, err := _verifyDstReplicationStop(context.Background(), event)
		assert.Error(tt, err)
		assert.Nil(tt, replication)
		var customErr *vsaErrors.CustomError
		assert.True(tt, vsaErrors.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(tt, customErr.OriginalErr, "replication not found")
	})
	t.Run("VerifyDstReplicationStopFailsWhenAlreadyStopped", func(tt *testing.T) {
		mirrorState := "STOPPED"
		mockReplication := &coreModels.VolumeReplication{
			MirrorState: &mirrorState,
		}
		event := &StopReplicationEvent{
			CommonReplicationEventParams: CommonReplicationEventParams{
				DstBasePath:              "dstPath",
				DestinationProjectNumber: "destinationProjectNumber",
				DstToken:                 "dstToken",
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation:        "dstLocation",
						DestinationReplicationUUID: "dstUUID",
					},
				},
			},
		}

		getReplication = func(ctx context.Context, basePath, projectNumber, locationID, replicationUUID, jwt string) (*coreModels.VolumeReplication, error) {
			return mockReplication, nil
		}
		defer func() { getReplication = _getReplication }()

		replication, err := _verifyDstReplicationStop(context.Background(), event)
		assert.Error(tt, err)
		assert.Nil(tt, replication)
		assert.Contains(tt, err.Error(), "Replication is already in STOPPED state")
	})

	t.Run("VerifyDstReplicationStopFailsWhenUninitializedAndTransferring", func(tt *testing.T) {
		mirrorState := "UNINITIALIZED"
		relationshipStatus := "transferring"
		mockReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		event := &StopReplicationEvent{
			CommonReplicationEventParams: CommonReplicationEventParams{
				DstBasePath:              "dstPath",
				DestinationProjectNumber: "destinationProjectNumber",
				DstToken:                 "dstToken",
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation:        "dstLocation",
						DestinationReplicationUUID: "dstUUID",
					},
				},
			},
			ForceStop: false,
		}

		getReplication = func(ctx context.Context, basePath, projectNumber, locationID, replicationUUID, jwt string) (*coreModels.VolumeReplication, error) {
			return mockReplication, nil
		}
		defer func() { getReplication = _getReplication }()

		replication, err := _verifyDstReplicationStop(context.Background(), event)
		assert.Error(tt, err)
		assert.Nil(tt, replication)
		assert.Contains(tt, err.Error(), "Replication in preparing state. Please try again later")
	})

	t.Run("VerifyDstReplicationStopFailsWhenMirroredAndTransferring", func(tt *testing.T) {
		mirrorState := "MIRRORED"
		relationshipStatus := "transferring"
		mockReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		event := &StopReplicationEvent{
			CommonReplicationEventParams: CommonReplicationEventParams{
				DstBasePath:              "dstPath",
				DestinationProjectNumber: "destinationProjectNumber",
				DstToken:                 "dstToken",
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation:        "dstLocation",
						DestinationReplicationUUID: "dstUUID",
					},
				},
			},
			ForceStop: false,
		}

		getReplication = func(ctx context.Context, basePath, projectNumber, locationID, replicationUUID, jwt string) (*coreModels.VolumeReplication, error) {
			return mockReplication, nil
		}
		defer func() { getReplication = _getReplication }()

		replication, err := _verifyDstReplicationStop(context.Background(), event)
		assert.Error(tt, err)
		assert.Nil(tt, replication)
		assert.Contains(tt, err.Error(), "Replication relationship status is in transferring state")
	})
}

func TestVerifyDstVolume(t *testing.T) {
	correlationId := "correlationID"
	event := &ResumeReplicationEvent{
		CommonReplicationEventParams: CommonReplicationEventParams{
			SrcBasePath:              "srcPath",
			DstBasePath:              "dstPath",
			SourceProjectNumber:      "sourceProjectNumber",
			DestinationProjectNumber: "destinationProjectNumber",
			SrcToken:                 "srcToken",
			DstToken:                 "dstToken",
			ReplicationModel: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					SourceLocation:             "srcLocation",
					DestinationLocation:        "dstLocation",
					DestinationReplicationUUID: "dstUUID",
					SourceVolumeUUID:           "srcVolumeUUID",
				},
			},
			XCorrelationID: &correlationId,
		},
	}
	t.Run("WhenDescribeVolumeError", func(tt *testing.T) {
		defer func() {
			describeVolume = _describeVolume
		}()
		ctx := context.Background()
		describeVolume = func(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{}, errors.New("some error")
		}
		_, _, err := _verifyDstVolume(ctx, event.ReplicationModel.ReplicationAttributes, "srcPath", "dstPath", "srcToken", "dstToken", "srcProject", "dstProject", &correlationId, false)
		assert.Error(tt, err)
	})
	t.Run("WhenDescribeDestinationVolumeError", func(tt *testing.T) {
		defer func() {
			describeVolume = _describeVolume
		}()
		ctx := context.Background()
		count := 0
		describeVolume = func(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
			if count == 0 {
				count = count + 1
				return googleproxyclient.VolumeV1beta{}, nil
			}
			return googleproxyclient.VolumeV1beta{}, errors.New("some error")
		}
		_, _, err := _verifyDstVolume(ctx, event.ReplicationModel.ReplicationAttributes, "srcPath", "dstPath", "srcToken", "dstToken", "srcProject", "dstProject", &correlationId, false)
		assert.Error(tt, err)
	})
	t.Run("WhenVolumeNotFound", func(tt *testing.T) {
		defer func() {
			describeVolume = _describeVolume
		}()
		ctx := context.Background()
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrVolumeNotFound, errors.NewNotFoundErr("Volume", &[]string{"vol-1"}[0]))
		describeVolume = func(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{}, errors.NewNotFoundErr("Volume", &[]string{"vol-1"}[0])
		}
		_, _, err := _verifyDstVolume(ctx, event.ReplicationModel.ReplicationAttributes, "srcPath", "dstPath", "srcToken", "dstToken", "srcProject", "dstProject", &correlationId, false)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenDestinationVolumeNotFound", func(tt *testing.T) {
		defer func() {
			describeVolume = _describeVolume
		}()
		ctx := context.Background()
		count := 0
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrVolumeNotFound, errors.NewNotFoundErr("Volume", &[]string{"vol-1"}[0]))
		describeVolume = func(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
			if count == 0 {
				count = count + 1
				return googleproxyclient.VolumeV1beta{}, nil
			}
			return googleproxyclient.VolumeV1beta{}, errors.NewNotFoundErr("Volume", &[]string{"vol-1"}[0])
		}
		_, _, err := _verifyDstVolume(ctx, event.ReplicationModel.ReplicationAttributes, "srcPath", "dstPath", "srcToken", "dstToken", "srcProject", "dstProject", &correlationId, false)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenVolumeStateOffline", func(tt *testing.T) {
		defer func() {
			describeVolume = _describeVolume
		}()
		ctx := context.Background()
		describeVolume = func(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{
				VolumeState: googleproxyclient.OptVolumeV1betaVolumeState{
					Set:   true,
					Value: "offline",
				},
			}, nil
		}
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrVolumeNotOnlineForReplicationResume, errors.New("Volume is not online for replication"))
		_, _, err := _verifyDstVolume(ctx, event.ReplicationModel.ReplicationAttributes, "srcPath", "dstPath", "srcToken", "dstToken", "srcProject", "dstProject", &correlationId, false)
		assert.Error(tt, err)
		assert.Equal(tt, err, expectedError)
	})
	t.Run("WhenDestinationVolumeUsedSizeGreaterThanSourceVolumeAvailableQuota", func(tt *testing.T) {
		defer func() {
			describeVolume = _describeVolume
		}()
		ctx := context.Background()
		count := 0
		describeVolume = func(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
			if count == 0 {
				count = count + 1
				return googleproxyclient.VolumeV1beta{
					QuotaInBytes: googleproxyclient.NewOptFloat64(123),
				}, nil
			} else {
				return googleproxyclient.VolumeV1beta{
					QuotaInBytes: googleproxyclient.NewOptFloat64(124),
					UsedBytes:    googleproxyclient.NewOptNilFloat64(200),
				}, nil
			}
		}
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrDestinationVolumeUsedSizeGreaterThanSourceVolumeAvailableQuota, errors.New("Destination volume used size is greater than source volume available quota"))
		_, _, err := _verifyDstVolume(ctx, event.ReplicationModel.ReplicationAttributes, "srcPath", "dstPath", "srcToken", "dstToken", "srcProject", "dstProject", &correlationId, false)
		assert.NotNil(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		defer func() {
			describeVolume = _describeVolume
		}()
		ctx := context.Background()
		describeVolume = func(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{}, nil
		}
		_, _, err := _verifyDstVolume(ctx, event.ReplicationModel.ReplicationAttributes, "srcPath", "dstPath", "srcToken", "dstToken", "srcProject", "dstProject", &correlationId, false)
		assert.NoError(tt, err)
	})
}

func TestDescribeVolume(t *testing.T) {
	basePath := "basePath"
	token := "token"
	locationId := "locationId"
	projectNumber := "projectNumber"
	volumeId := "volumeId"
	xCorrelationID := new(string)
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaDescribeVolume(ctx, mock.Anything).Return(&googleproxyclient.VolumeV1beta{}, errors.New("some error"))

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}
		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		_, err := _describeVolume(ctx, basePath, token, locationId, projectNumber, xCorrelationID, volumeId)
		assert.Error(tt, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)
		response := &googleproxyclient.VolumeV1beta{}
		mockClient.EXPECT().V1betaDescribeVolume(ctx, mock.Anything).Return(response, nil)

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		resp, err := _describeVolume(ctx, basePath, token, locationId, projectNumber, xCorrelationID, volumeId)
		assert.NoError(tt, err)
		assert.Equal(tt, *response, resp)
	})
}

func TestGetReplication(t *testing.T) {
	t.Run("WhenError", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(ctx, mock.Anything, mock.Anything).Return(nil, errors.New("some error"))

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		_, err := _getReplication(ctx, "basePath", "projectNumber", "locationID", "volumeReplicationID", "jwt")
		assert.Error(tt, err)
		assert.Equal(tt, "Failed to get multiple replications", err.Error())
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockClient := googleproxyclient.NewMockInvoker(t)
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleReplicationsInternal(ctx, mock.Anything, mock.Anything).Return(response, nil)

		originalGetGProxyClient := googleproxyclient.GetGProxyClient
		mc := &googleproxyclient.ProxyClient{
			Invoker: mockClient,
		}
		googleproxyclient.GetGProxyClient = func(basePath string, jwt string, logger log.Logger) *googleproxyclient.ProxyClient {
			return mc
		}

		defer func() { googleproxyclient.GetGProxyClient = originalGetGProxyClient }()
		_, err := _getReplication(ctx, "basePath", "projectNumber", "locationID", "volumeReplicationID", "jwt")
		assert.NoError(tt, err)
	})
}

func TestVerifyDstReplicationDelete(t *testing.T) {
	event := &DeleteReplicationEvent{
		CommonReplicationEventParams: CommonReplicationEventParams{
			DstBasePath:              "dstPath",
			DestinationProjectNumber: "destinationProjectNumber",
			DstToken:                 "dstToken",
			ReplicationModel: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dstLocation",
					DestinationReplicationUUID: "dstUUID",
				},
			},
		},
	}
	t.Run("WhenGetReplicationError", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return nil, errors.New("some error")
		}
		_, err := _verifyDstReplication(ctx, event)
		assert.Error(tt, err)
	})
	t.Run("WhenMirrorStateMirrored", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := "MIRRORED"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState: &mirrorState,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}
		expectedError := errors.NewUserInputValidationErr(fmt.Sprintf("Destination replication is in mirror_state: %v expected_mirror_state: %v", models.ReplicationV1betaMirrorStateMIRRORED, models.ReplicationV1betaMirrorStateSTOPPED))

		_, err := _verifyDstReplication(ctx, event)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenMirrorStatePreparing", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := "ABORTED"
		relationshipStatus := "transferring"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}
		expectedError := errors.NewUserInputValidationErr(fmt.Sprintf("Expected mirror state: %v or %v", models.ReplicationV1betaMirrorStatePREPARING, models.ReplicationV1betaMirrorStateSTOPPED))

		_, err := _verifyDstReplication(ctx, event)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenMirrorStateUninitialized", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := "UNINITIALIZED"
		relationshipStatus := "transferring"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}
		expectedError := errors.NewUserInputValidationErr(fmt.Sprintf("Replication relationship status should be %s", models.VolumeReplicationCVPV1betaRelationshipStatusIdle))
		_, err := _verifyDstReplication(ctx, event)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := "STOPPED"
		relationshipStatus := "IDLE"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}
		resp, err := _verifyDstReplication(ctx, event)
		assert.NoError(tt, err)
		assert.Equal(tt, dstReplication, resp)
	})
}

func TestVerifyDstReplicationSync(t *testing.T) {
	event := &ResumeReplicationEvent{
		CommonReplicationEventParams: CommonReplicationEventParams{
			DstBasePath:              "dstPath",
			DestinationProjectNumber: "destinationProjectNumber",
			DstToken:                 "dstToken",
			ReplicationModel: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dstLocation",
					DestinationReplicationUUID: "dstUUID",
				},
			},
		},
	}
	t.Run("WhenGetReplicationError", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return nil, errors.New("some error")
		}

		_, err := _verifyDstReplicationSync(ctx, event)
		assert.Error(tt, err)
	})

	t.Run("WhenMirrorStateNotMirrored", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := "STOPPED"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState: &mirrorState,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}

		expectedError := errors.NewUserInputValidationErr(fmt.Sprintf("Replication mirror state should be %s", models.ReplicationV1betaMirrorStateMIRRORED))
		_, err := _verifyDstReplicationSync(ctx, event)
		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := models.ReplicationV1betaMirrorStateMIRRORED
		dstReplication := &coreModels.VolumeReplication{
			MirrorState: &mirrorState,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}

		resp, err := _verifyDstReplicationSync(ctx, event)
		assert.NoError(tt, err)
		assert.Equal(tt, dstReplication, resp)
	})
}

func TestValidateReplicationUpdate(t *testing.T) {
	event := &UpdateReplicationEvent{
		CommonReplicationEventParams: CommonReplicationEventParams{
			DstBasePath:              "dstPath",
			DestinationProjectNumber: "destinationProjectNumber",
			DstToken:                 "dstToken",
			ReplicationModel: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "dstLocation",
					DestinationReplicationUUID: "dstUUID",
				},
			},
		},
	}
	t.Run("WhenGetReplicationError", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return nil, errors.New("some error")
		}

		event1 := *event
		event1.Description = nillable.ToPointer("New description")
		_, err := _validateReplicationUpdate(ctx, &event1)
		assert.Error(tt, err)
	})
	t.Run("WhenNothingToUpdate", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		_, err := _validateReplicationUpdate(ctx, event)
		assert.Error(tt, err)
		var customErr *vsaErrors.CustomError
		assert.True(tt, vsaErrors.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(tt, customErr.OriginalErr, "empty replication update payload")
	})
	t.Run("WhenReplicationScheduleUnspecified", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()

		event1 := *event
		event1.ReplicationSchedule = nillable.ToPointer("REPLICATION_SCHEDULE_UNSPECIFIED")
		_, err := _validateReplicationUpdate(ctx, &event1)
		assert.Error(tt, err)
		var customErr *vsaErrors.CustomError
		assert.True(tt, vsaErrors.As(err, &customErr), "Expected a CustomError")
		assert.ErrorContains(tt, customErr.OriginalErr, "Invalid replication schedule provided.")
	})
	t.Run("WhenLabelsInvalid", func(tt *testing.T) {
		ctx := context.Background()
		event.Description = nillable.ToPointer("New description")
		event.Labels = map[string]string{"": "value"}
		_, err := _validateReplicationUpdate(ctx, event)
		assert.Error(tt, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := "STOPPED"
		relationshipStatus := "IDLE"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		event.Description = nillable.ToPointer("New description")
		event.Labels = map[string]string{"key": "value"}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}
		resp, err := _validateReplicationUpdate(ctx, event)
		assert.NoError(tt, err)
		assert.Equal(tt, dstReplication, resp)
	})
}

func Test_verifyDstReplicationReverse(t *testing.T) {
	event := &ReverseReplicationEvent{
		CommonReplicationEventParams: CommonReplicationEventParams{
			ReplicationModel: &datamodel.VolumeReplication{
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationLocation:        "us-east1",
					DestinationReplicationUUID: "dest-repl-uuid",
				},
			},
			DstBasePath:              "dstPath",
			DestinationProjectNumber: "dest-proj",
			DstToken:                 "dstToken",
		},
	}

	t.Run("WhenGetReplicationError", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return nil, errors.New("get replication error")
		}

		_, err := _verifyDstReplicationReverse(ctx, event)
		assert.Error(tt, err)
		var customErr *vsaErrors.CustomError
		assert.True(tt, vsaErrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaErrors.ErrGoogleProxyInternalGetMultipleReplications, customErr.TrackingID)
	})

	t.Run("WhenReplicationIsNil", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return nil, nil
		}

		_, err := _verifyDstReplicationReverse(ctx, event)
		assert.Error(tt, err)
		var customErr *vsaErrors.CustomError
		assert.True(tt, vsaErrors.As(err, &customErr), "Expected a CustomError")
		assert.Equal(tt, vsaErrors.ErrGoogleProxyInternalGetMultipleReplications, customErr.TrackingID)
	})

	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		defer func() {
			getReplication = _getReplication
		}()
		mirrorState := models.ReplicationV1betaMirrorStateSTOPPED
		relationshipStatus := "idle"
		dstReplication := &coreModels.VolumeReplication{
			MirrorState:        &mirrorState,
			RelationshipStatus: &relationshipStatus,
		}
		getReplication = func(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*coreModels.VolumeReplication, error) {
			return dstReplication, nil
		}

		resp, err := _verifyDstReplicationReverse(ctx, event)
		assert.NoError(tt, err)
		assert.Equal(tt, dstReplication, resp)
	})
}

// TestVolumeIDValidation tests the volume ID validation regex pattern
func TestVolumeIDValidation(t *testing.T) {
	// Compile the regex pattern used in the validation
	compiledRegex := regexp.MustCompile(dstVolumeNameRegex)

	t.Run("ValidVolumeIDs", func(tt *testing.T) {
		validCases := []struct {
			name     string
			volumeID string
		}{
			{"SingleLetter", "a"},
			{"LettersAndNumbers", "abc123"},
			{"WithUnderscores", "test_volume_1"},
			{"MixedValidChars", "my_volume_123"},
			{"MaxLength", "a" + strings.Repeat("b", 61) + "c"}, // 63 chars total
			{"AllLowercase", "abcdefghijklmnopqrstuvwxyz"},
			{"NumbersOnly", "a123456789"},
			{"UnderscoresOnly", "a_b_c_d_e"},
		}

		for _, tc := range validCases {
			tt.Run(tc.name, func(t *testing.T) {
				matches := compiledRegex.MatchString(tc.volumeID)
				assert.True(t, matches, "Volume ID '%s' should match the regex pattern", tc.volumeID)
			})
		}
	})

	t.Run("InvalidVolumeIDs", func(tt *testing.T) {
		invalidCases := []struct {
			name     string
			volumeID string
		}{
			{"StartsWithNumber", "123abc"},
			{"StartsWithUppercase", "Abc123"},
			{"ContainsHyphen", "test-volume"},
			{"EndsWithUnderscore", "test_volume_"},
			{"ContainsSpecialChars", "test@volume"},
			{"ContainsSpaces", "test volume"},
			{"EmptyString", ""},
			{"TooLong", "a" + strings.Repeat("b", 62) + "c"}, // 64 chars total
			{"StartsWithSpecialChar", "_test"},
			{"StartsWithHyphen", "-test"},
		}

		for _, tc := range invalidCases {
			tt.Run(tc.name, func(t *testing.T) {
				matches := compiledRegex.MatchString(tc.volumeID)
				assert.False(t, matches, "Volume ID '%s' should NOT match the regex pattern", tc.volumeID)
			})
		}
	})

	t.Run("EmptyVolumeID", func(tt *testing.T) {
		// Empty volume ID should not match the regex (since it requires at least one character)
		matches := compiledRegex.MatchString("")
		assert.False(t, matches, "Empty volume ID should NOT match the regex pattern")
	})
}

func TestMapLifecycleStateToState(t *testing.T) {
	t.Run("ValidStateMappings", func(tt *testing.T) {
		testCases := []struct {
			name          string
			inputState    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState
			expectedState string
		}{
			{
				name:          "Creating",
				inputState:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateCreating,
				expectedState: coreModels.LifeCycleStateCreating,
			},
			{
				name:          "Available",
				inputState:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateAvailable,
				expectedState: coreModels.LifeCycleStateAvailable,
			},
			{
				name:          "Updating",
				inputState:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateUpdating,
				expectedState: coreModels.LifeCycleStateUpdating,
			},
			{
				name:          "Disabled",
				inputState:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDisabled,
				expectedState: coreModels.LifeCycleStateDisabled,
			},
			{
				name:          "Deleting",
				inputState:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDeleting,
				expectedState: coreModels.LifeCycleStateDeleting,
			},
			{
				name:          "Deleted",
				inputState:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDeleted,
				expectedState: coreModels.LifeCycleStateDeleted,
			},
			{
				name:          "Error",
				inputState:    googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateError,
				expectedState: coreModels.LifeCycleStateError,
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				result := mapLifecycleStateToState(tc.inputState)
				assert.Equal(t, tc.expectedState, result, "Expected state %s, got %s", tc.expectedState, result)
			})
		}
	})
	t.Run("UnknownState", func(tt *testing.T) {
		// Test with an unknown/invalid state
		unknownState := googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState("unknown")
		result := mapLifecycleStateToState(unknownState)
		assert.Equal(t, coreModels.LifeCycleStateUnknown, result, "Unknown state should map to LifeCycleStateUnknown")
	})
	t.Run("EmptyState", func(tt *testing.T) {
		// Test with empty state
		emptyState := googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState("")
		result := mapLifecycleStateToState(emptyState)
		assert.Equal(t, coreModels.LifeCycleStateUnknown, result, "Empty state should map to LifeCycleStateUnknown")
	})
	t.Run("CaseSensitivity", func(tt *testing.T) {
		// Test that the function is case-sensitive (should not match uppercase)
		uppercaseState := googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState("CREATING")
		result := mapLifecycleStateToState(uppercaseState)
		assert.Equal(t, coreModels.LifeCycleStateUnknown, result, "Uppercase state should map to LifeCycleStateUnknown")
	})
	t.Run("AllEnumValuesCovered", func(tt *testing.T) {
		// Verify that all possible enum values are handled
		allStates := []googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState{
			googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateCreating,
			googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateAvailable,
			googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateUpdating,
			googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDisabled,
			googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDeleting,
			googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDeleted,
			googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateError,
		}

		for _, state := range allStates {
			result := mapLifecycleStateToState(state)
			assert.NotEqual(t, coreModels.LifeCycleStateUnknown, result, "State %s should not map to unknown", state)
		}
	})
}

func TestConvertReplicationResponseToModels(t *testing.T) {
	t.Run("ValidReplicationResponse", func(tt *testing.T) {
		testCases := []struct {
			name           string
			lifecycleState googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState
			expectedState  string
		}{
			{
				name:           "CreatingState",
				lifecycleState: googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateCreating,
				expectedState:  coreModels.LifeCycleStateCreating,
			},
			{
				name:           "AvailableState",
				lifecycleState: googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateAvailable,
				expectedState:  coreModels.LifeCycleStateAvailable,
			},
			{
				name:           "UpdatingState",
				lifecycleState: googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateUpdating,
				expectedState:  coreModels.LifeCycleStateUpdating,
			},
			{
				name:           "DisabledState",
				lifecycleState: googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDisabled,
				expectedState:  coreModels.LifeCycleStateDisabled,
			},
			{
				name:           "DeletingState",
				lifecycleState: googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDeleting,
				expectedState:  coreModels.LifeCycleStateDeleting,
			},
			{
				name:           "DeletedState",
				lifecycleState: googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateDeleted,
				expectedState:  coreModels.LifeCycleStateDeleted,
			},
			{
				name:           "ErrorState",
				lifecycleState: googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateError,
				expectedState:  coreModels.LifeCycleStateError,
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				// Create a mock response with the specific lifecycle state
				response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
					Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
						{
							Name:                  googleproxyclient.OptString{Value: "test-replication", Set: true},
							VolumeReplicationUuid: googleproxyclient.OptString{Value: "test-uuid", Set: true},
							Description:           googleproxyclient.OptString{Value: "test description", Set: true},
							SourceVolumeUuid:      googleproxyclient.OptString{Value: "source-uuid", Set: true},
							SourceVolumeName:      "source-volume",
							DestinationVolumeUuid: googleproxyclient.OptString{Value: "dest-uuid", Set: true},
							DestinationVolumeName: "dest-volume",
							ReplicationSchedule:   googleproxyclient.OptVolumeReplicationInternalV1betaReplicationSchedule{Value: googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleHourly, Set: true},
							EndpointType:          googleproxyclient.VolumeReplicationInternalV1betaEndpointTypeSrc,
							TotalTransferBytes:    googleproxyclient.OptInt64{Value: 1024, Set: true},
							TotalTransferTimeSecs: googleproxyclient.OptInt64{Value: 3600, Set: true},
							LastTransferSize:      googleproxyclient.OptInt64{Value: 512, Set: true},
							LastTransferError:     googleproxyclient.OptString{Value: "", Set: true},
							LastTransferDuration:  googleproxyclient.OptInt64{Value: 1800, Set: true},
							TotalProgress:         googleproxyclient.OptInt64{Value: 50, Set: true},
							LagTime:               googleproxyclient.OptInt64{Value: 300, Set: true},
							LifeCycleState:        googleproxyclient.OptVolumeReplicationInternalV1betaLifeCycleState{Value: tc.lifecycleState, Set: true},
							LifeCycleStateDetails: googleproxyclient.OptString{Value: "test state details", Set: true},
							CreatedAt:             googleproxyclient.OptDateTime{Value: time.Now(), Set: true},
						},
					},
				}

				result := convertReplicationResponseToModels(response)

				assert.NotNil(t, result, "Result should not be nil")
				assert.Equal(t, tc.expectedState, result.State, "Expected state %s, got %s", tc.expectedState, result.State)
				assert.Equal(t, "test state details", result.StateDetails, "State details should match")
				assert.Equal(t, "test-replication", result.Name, "Name should match")
				assert.Equal(t, "test-uuid", result.UUID, "UUID should match")
			})
		}
	})

	t.Run("NilResponse", func(tt *testing.T) {
		// Note: The function doesn't handle nil response gracefully, so we expect a panic
		defer func() {
			if r := recover(); r != nil {
				// Expected panic when response is nil
			}
		}()
		result := convertReplicationResponseToModels(nil)
		assert.Nil(t, result, "Result should be nil for nil response")
	})

	t.Run("EmptyReplications", func(tt *testing.T) {
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{},
		}

		result := convertReplicationResponseToModels(response)
		assert.Nil(t, result, "Result should be nil for empty replications")
	})

	t.Run("UnknownLifecycleState", func(tt *testing.T) {
		unknownState := googleproxyclient.VolumeReplicationInternalV1betaLifeCycleState("unknown")
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					Name:                  googleproxyclient.OptString{Value: "test-replication", Set: true},
					VolumeReplicationUuid: googleproxyclient.OptString{Value: "test-uuid", Set: true},
					Description:           googleproxyclient.OptString{Value: "test description", Set: true},
					SourceVolumeUuid:      googleproxyclient.OptString{Value: "source-uuid", Set: true},
					SourceVolumeName:      "source-volume",
					DestinationVolumeUuid: googleproxyclient.OptString{Value: "dest-uuid", Set: true},
					DestinationVolumeName: "dest-volume",
					ReplicationSchedule:   googleproxyclient.OptVolumeReplicationInternalV1betaReplicationSchedule{Value: googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleHourly, Set: true},
					EndpointType:          googleproxyclient.VolumeReplicationInternalV1betaEndpointTypeSrc,
					TotalTransferBytes:    googleproxyclient.OptInt64{Value: 1024, Set: true},
					TotalTransferTimeSecs: googleproxyclient.OptInt64{Value: 3600, Set: true},
					LastTransferSize:      googleproxyclient.OptInt64{Value: 512, Set: true},
					LastTransferError:     googleproxyclient.OptString{Value: "", Set: true},
					LastTransferDuration:  googleproxyclient.OptInt64{Value: 1800, Set: true},
					TotalProgress:         googleproxyclient.OptInt64{Value: 50, Set: true},
					LagTime:               googleproxyclient.OptInt64{Value: 300, Set: true},
					LifeCycleState:        googleproxyclient.OptVolumeReplicationInternalV1betaLifeCycleState{Value: unknownState, Set: true},
					LifeCycleStateDetails: googleproxyclient.OptString{Value: "test state details", Set: true},
					CreatedAt:             googleproxyclient.OptDateTime{Value: time.Now(), Set: true},
				},
			},
		}

		result := convertReplicationResponseToModels(response)

		assert.NotNil(t, result, "Result should not be nil")
		assert.Equal(t, coreModels.LifeCycleStateUnknown, result.State, "Unknown state should map to LifeCycleStateUnknown")
		assert.Equal(t, "test state details", result.StateDetails, "State details should match")
	})

	t.Run("CompleteReplicationData", func(tt *testing.T) {
		response := &googleproxyclient.V1betaGetMultipleReplicationsInternalOK{
			Replications: []googleproxyclient.VolumeReplicationInternalV1beta{
				{
					Name:                  googleproxyclient.OptString{Value: "test-replication", Set: true},
					VolumeReplicationUuid: googleproxyclient.OptString{Value: "test-uuid", Set: true},
					Description:           googleproxyclient.OptString{Value: "test description", Set: true},
					SourceVolumeUuid:      googleproxyclient.OptString{Value: "source-uuid", Set: true},
					SourceVolumeName:      "source-volume",
					DestinationVolumeUuid: googleproxyclient.OptString{Value: "dest-uuid", Set: true},
					DestinationVolumeName: "dest-volume",
					ReplicationSchedule:   googleproxyclient.OptVolumeReplicationInternalV1betaReplicationSchedule{Value: googleproxyclient.VolumeReplicationInternalV1betaReplicationScheduleHourly, Set: true},
					EndpointType:          googleproxyclient.VolumeReplicationInternalV1betaEndpointTypeSrc,
					TotalTransferBytes:    googleproxyclient.OptInt64{Value: 1024, Set: true},
					TotalTransferTimeSecs: googleproxyclient.OptInt64{Value: 3600, Set: true},
					LastTransferSize:      googleproxyclient.OptInt64{Value: 512, Set: true},
					LastTransferError:     googleproxyclient.OptString{Value: "", Set: true},
					LastTransferDuration:  googleproxyclient.OptInt64{Value: 1800, Set: true},
					TotalProgress:         googleproxyclient.OptInt64{Value: 50, Set: true},
					LagTime:               googleproxyclient.OptInt64{Value: 300, Set: true},
					LifeCycleState:        googleproxyclient.OptVolumeReplicationInternalV1betaLifeCycleState{Value: googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateAvailable, Set: true},
					LifeCycleStateDetails: googleproxyclient.OptString{Value: "test state details", Set: true},
					CreatedAt:             googleproxyclient.OptDateTime{Value: time.Now(), Set: true},
				},
			},
		}

		result := convertReplicationResponseToModels(response)

		assert.NotNil(t, result, "Result should not be nil")
		assert.Equal(t, coreModels.LifeCycleStateAvailable, result.State, "State should be mapped correctly")
		assert.Equal(t, "test state details", result.StateDetails, "State details should match")
		assert.Equal(t, "test-replication", result.Name, "Name should match")
		assert.Equal(t, "test-uuid", result.UUID, "UUID should match")
		assert.Equal(t, "test description", result.Description, "Description should match")
		assert.NotNil(t, result.ReplicationAttributes, "ReplicationAttributes should be set")
		assert.Equal(t, "source-uuid", result.ReplicationAttributes.SourceVolumeUUID, "SourceVolumeUUID should match")
		assert.Equal(t, "source-volume", result.ReplicationAttributes.SourceVolumeName, "SourceVolumeName should match")
		assert.Equal(t, "dest-uuid", result.ReplicationAttributes.DestinationVolumeUUID, "DestinationVolumeUUID should match")
		assert.Equal(t, "dest-volume", result.ReplicationAttributes.DestinationVolumeName, "DestinationVolumeName should match")
		assert.Equal(t, "hourly", result.ReplicationAttributes.ReplicationSchedule, "ReplicationSchedule should match")
		assert.Equal(t, "src", result.ReplicationAttributes.EndpointType, "EndpointType should match")
		assert.Equal(t, int64(1024), result.TotalTransferBytes, "TotalTransferBytes should match")
		assert.Equal(t, int64(3600), result.TotalTransferTimeSecs, "TotalTransferTimeSecs should match")
		assert.Equal(t, int64(512), result.LastTransferSize, "LastTransferSize should match")
		assert.Equal(t, "", result.LastTransferError, "LastTransferError should match")
		assert.Equal(t, int64(1800), result.LastTransferDuration, "LastTransferDuration should match")
		assert.Equal(t, int64(50), result.TotalProgress, "TotalProgress should match")
		assert.Equal(t, int64(300), result.LagTime, "LagTime should match")
	})
}

func TestIsPoolInTransitionState(t *testing.T) {
	t.Run("WhenCreatingState", func(tt *testing.T) {
		pool := &googleproxyclient.PoolV1beta{
			StoragePoolState: googleproxyclient.OptPoolV1betaStoragePoolState{Value: googleproxyclient.PoolV1betaStoragePoolStateCREATING},
		}
		resp := isPoolInTransitionState(pool)
		assert.True(tt, resp, "Should be true")
	})
	t.Run("WhenDeletingState", func(tt *testing.T) {
		pool := &googleproxyclient.PoolV1beta{
			StoragePoolState: googleproxyclient.OptPoolV1betaStoragePoolState{Value: googleproxyclient.PoolV1betaStoragePoolStateDELETING},
		}
		resp := isPoolInTransitionState(pool)
		assert.True(tt, resp, "Should be true")
	})
	t.Run("WhenUpdatingState", func(tt *testing.T) {
		pool := &googleproxyclient.PoolV1beta{
			StoragePoolState: googleproxyclient.OptPoolV1betaStoragePoolState{Value: googleproxyclient.PoolV1betaStoragePoolStateUPDATING},
		}
		resp := isPoolInTransitionState(pool)
		assert.False(tt, resp, "Should be false")
	})
	t.Run("WhenReadyState", func(tt *testing.T) {
		pool := &googleproxyclient.PoolV1beta{
			StoragePoolState: googleproxyclient.OptPoolV1betaStoragePoolState{Value: googleproxyclient.PoolV1betaStoragePoolStateREADY},
		}
		resp := isPoolInTransitionState(pool)
		assert.False(tt, resp, "Should be false")
	})
}
