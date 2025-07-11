package replication

import (
	"context"
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
	replicationSchedule := models.ReplicationV1betaReplicationScheduleREPLICATIONSCHEDULEUNSPECIFIED
	volumeID := "vol-2"
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
	assert.Contains(t, replication.RemoteUri, "projects/dst-proj/locations/loc-2/volumes/vol-2/replications/replication-1")
	assert.Equal(t, resourceID, replication.Name)
	assert.Equal(t, description, replication.Description)
	assert.NotNil(t, replication.ReplicationAttributes)
	assert.Equal(t, event.SourceVolume.UUID, replication.ReplicationAttributes.SourceVolumeUUID)
	assert.Equal(t, "vol-1", replication.ReplicationAttributes.SourceVolumeName)
	assert.Equal(t, "region-1", replication.ReplicationAttributes.SourceLocation)
	assert.Equal(t, "region-2", replication.ReplicationAttributes.DestinationLocation)
	assert.Equal(t, models.VolumeReplicationCVPV1betaEndpointTypeSrc, replication.ReplicationAttributes.EndpointType)
	assert.Equal(t, replicationSchedule, replication.ReplicationAttributes.ReplicationSchedule)
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
				VolumeID: "vol-2",
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
	validLabels := map[string]string{
		"env":    "prod",
		"region": "us-central1",
	}

	// Valid case
	err := _validateLabels(validLabels)
	assert.NoError(t, err)

	// Valid: empty map
	err = _validateLabels(map[string]string{})
	assert.NoError(t, err)

	// Too many labels
	tooMany := map[string]string{}
	for i := 0; i < 65; i++ {
		tooMany[fmt.Sprintf("k%d", i)] = "v"
	}
	err = _validateLabels(tooMany)
	assert.Error(t, err)
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
		assert.Contains(t, err.Error(), "volume not found")
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
	destProjectNumber := "proj-2"
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
				VolumeID:    "vol-2",
				ShareName:   "share-2",
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
			return googleproxyclient.VolumeV1beta{}, errors.New("volume not found")
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

	t.Run("Fails when GetSignedToken fails", func(t *testing.T) {
		mockStorage := &database.MockStorage{}
		origGetSignedJwtToken := InternalUtilGetSignedToken
		InternalUtilGetSignedToken = func(p string) (string, error) { return "", errors.New("token error") }
		defer func() { InternalUtilGetSignedToken = origGetSignedJwtToken }()

		_, err := _validateCreateReplicationParams(ctx, event, mockStorage)
		assert.Error(t, err)
	})

	// More negative test cases can be added for each error path as needed
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
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		expectedError := vsaErrors.NewVCPError(vsaErrors.ErrDatabaseDataReadError, errors.New("some error"))
		err := _validateReplicationParams(context.Background(), event, 12345, mockStorage)

		assert.Error(tt, err)
		assert.Equal(tt, expectedError, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenEmptyListVolumeReplication", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything).Return(response, nil)
		expectedError := errors.NewUserInputValidationErr("No replication found for the given URI")
		err := _validateReplicationParams(context.Background(), event, 12345, mockStorage)
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
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything).Return(response, nil)

		parseError := errors.New("[0] undefined error: some error")
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", vsaErrors.NewVCPError(vsaErrors.ErrProjectParsingError, parseError)
		}
		err := _validateReplicationParams(context.Background(), event, 12345, mockStorage)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSignedTokenError", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", vsaErrors.NewVCPError(vsaErrors.ErrGetSignedToken, errors.New("some error"))
		}
		err := _validateReplicationParams(context.Background(), event, 12345, mockStorage)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenGetPairedRegionError", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "", errors.New("some error")
		}
		err := _validateReplicationParams(context.Background(), event, 12345, mockStorage)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenReplicationJobInProcessError", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			replicationJobInProcess = _replicationJobInProcess
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		replicationJobInProcess = func(ctx context.Context, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId string, correlationId *string) error {
			return errors.New("some error")
		}
		err := _validateReplicationParams(context.Background(), event, 12345, mockStorage)
		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		defer func() {
			utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
			InternalUtilGetSignedToken = auth.GetSignedJwtToken
			InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
			replicationJobInProcess = _replicationJobInProcess
		}()
		mockStorage := database.NewMockStorage(tt)
		response := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				ReplicationAttributes: &datamodel.ReplicationDetails{},
			},
		}
		mockStorage.On("ListVolumeReplications", mock.Anything, mock.Anything).Return(response, nil)
		utilsParseProjectNumberFromURI = func(uri string) (string, error) {
			return "", nil
		}
		InternalUtilGetSignedToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		InternalUtilGetPairedRegionURI = func(region string) (string, error) {
			return "basePath", nil
		}
		replicationJobInProcess = func(ctx context.Context, srcProjectNumber, destProjectNumber, srcBasePath, destBasePath, srcLocationID, destLocationID, srcToken, destToken, ccfeUri, remoteCcfeUri, srcPoolId, dstPoolId string, correlationId *string) error {
			return nil
		}
		err := _validateReplicationParams(context.Background(), event, 12345, mockStorage)
		assert.NoError(tt, err)
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
		assert.Contains(tt, err.Error(), "replication not found")
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
		_, _, err := _verifyDstVolume(ctx, event, "srcPath", "dstPath", "srcToken", "dstToken")
		assert.Error(tt, err)
	})
	t.Run("WhenVolumeNotFound", func(tt *testing.T) {
		defer func() {
			describeVolume = _describeVolume
		}()
		ctx := context.Background()
		describeVolume = func(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{}, errors.New("volume not found")
		}
		_, _, err := _verifyDstVolume(ctx, event, "srcPath", "dstPath", "srcToken", "dstToken")
		assert.Error(tt, err)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		defer func() {
			describeVolume = _describeVolume
		}()
		ctx := context.Background()
		describeVolume = func(ctx context.Context, basePath string, token string, locationID string, projectNumber string, xCorrelationID *string, volumeId string) (googleproxyclient.VolumeV1beta, error) {
			return googleproxyclient.VolumeV1beta{}, nil
		}
		_, _, err := _verifyDstVolume(ctx, event, "srcPath", "dstPath", "srcToken", "dstToken")
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
